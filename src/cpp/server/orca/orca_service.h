//
// Copyright 2024 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef GRPC_SRC_CPP_SERVER_ORCA_ORCA_SERVICE_H
#define GRPC_SRC_CPP_SERVER_ORCA_ORCA_SERVICE_H

#include <grpc/event_engine/event_engine.h>
#include <grpcpp/ext/orca_service.h>
#include <grpcpp/ext/server_metric_recorder.h>
#include <grpcpp/impl/rpc_method.h>
#include <grpcpp/impl/rpc_service_method.h>
#include <grpcpp/impl/server_callback_handlers.h>
#include <grpcpp/impl/sync.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/slice.h>
#include <grpcpp/support/status.h>
#include <stddef.h>

#include <memory>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "google/protobuf/duration.upb.h"
#include "src/core/lib/event_engine/default_event_engine.h"
#include "src/core/lib/iomgr/exec_ctx.h"
#include "src/core/util/debug_location.h"
#include "src/core/util/ref_counted.h"
#include "src/core/util/ref_counted_ptr.h"
#include "src/core/util/time.h"
#include "upb/mem/arena.hpp"
#include "xds/service/orca/v3/orca.upb.h"

namespace grpc {
namespace experimental {

class ReactorHook {
 public:
  virtual ~ReactorHook() = default;
  virtual void OnFinish(grpc::Status status) = 0;
  virtual void OnStartWrite(const ByteBuffer* response) = 0;
};

//
// OrcaService::Reactor
//

class OrcaService::Reactor
    : public ServerWriteReactor<ByteBuffer>,
      public grpc_core::RefCounted<OrcaService::Reactor> {
 public:
  explicit Reactor(OrcaService* service, absl::string_view peer,
                   const ByteBuffer* request_buffer,
                   std::shared_ptr<ReactorHook> hook)
      : RefCounted("OrcaService::Reactor"),
        service_(service),
        hook_(std::move(hook)),
        engine_(grpc_event_engine::experimental::GetDefaultEventEngine()) {
    // Get slice from request.
    Slice slice;
    grpc::Status status = request_buffer->DumpToSingleSlice(&slice);
    if (!status.ok()) {
      LOG_EVERY_N_SEC(WARNING, 1)
          << "OrcaService failed to extract request from peer: " << peer
          << " error:" << status.error_message();
      FinishRpc(Status(StatusCode::INTERNAL, status.error_message()));
      return;
    }
    // Parse request proto.
    upb::Arena arena;
    xds_service_orca_v3_OrcaLoadReportRequest* request =
        xds_service_orca_v3_OrcaLoadReportRequest_parse(
            reinterpret_cast<const char*>(slice.begin()), slice.size(),
            arena.ptr());
    if (request == nullptr) {
      LOG_EVERY_N_SEC(WARNING, 1)
          << "OrcaService failed to parse request proto from peer: " << peer;
      FinishRpc(Status(StatusCode::INTERNAL, "could not parse request proto"));
      return;
    }
    const auto* duration_proto =
        xds_service_orca_v3_OrcaLoadReportRequest_report_interval(request);
    if (duration_proto != nullptr) {
      report_interval_ = grpc_core::Duration::FromSecondsAndNanoseconds(
          google_protobuf_Duration_seconds(duration_proto),
          google_protobuf_Duration_nanos(duration_proto));
    }
    auto min_interval = grpc_core::Duration::Milliseconds(
        service_->min_report_duration_ / absl::Milliseconds(1));
    if (report_interval_ < min_interval) report_interval_ = min_interval;
    // Send initial response.
    SendResponse();
  }

  void OnWriteDone(bool ok) override {
    if (!ok) {
      FinishRpc(Status(StatusCode::UNKNOWN, "write failed"));
      return;
    }
    response_.Clear();
    if (!MaybeScheduleTimer()) {
      FinishRpc(Status(StatusCode::UNKNOWN, "call cancelled by client"));
    }
  }

  void OnCancel() override {
    if (MaybeCancelTimer()) {
      FinishRpc(Status(StatusCode::UNKNOWN, "call cancelled by client"));
    }
  }

  void OnDone() override {
    // Free the initial ref from instantiation.
    Unref(DEBUG_LOCATION, "OnDone");
  }

 private:
  void FinishRpc(grpc::Status status) {
    if (hook_ != nullptr) {
      hook_->OnFinish(status);
    }
    Finish(status);
  }

  void SendResponse() {
    Slice response_slice = service_->GetOrCreateSerializedResponse();
    ByteBuffer response_buffer(&response_slice, 1);
    response_.Swap(&response_buffer);
    if (hook_ != nullptr) {
      hook_->OnStartWrite(&response_);
    }
    StartWrite(&response_);
  }

  bool MaybeScheduleTimer() {
    grpc::internal::MutexLock lock(&timer_mu_);
    if (cancelled_) return false;
    timer_handle_ = engine_->RunAfter(
        report_interval_,
        [self = Ref(DEBUG_LOCATION, "Orca Service")] { self->OnTimer(); });
    return true;
  }

  bool MaybeCancelTimer() {
    grpc::internal::MutexLock lock(&timer_mu_);
    cancelled_ = true;
    if (timer_handle_.has_value() && engine_->Cancel(*timer_handle_)) {
      timer_handle_.reset();
      return true;
    }
    return false;
  }

  void OnTimer() {
    grpc_core::ApplicationCallbackExecCtx callback_exec_ctx;
    grpc_core::ExecCtx exec_ctx;
    grpc::internal::MutexLock lock(&timer_mu_);
    timer_handle_.reset();
    SendResponse();
  }

  OrcaService* service_;

  grpc::internal::Mutex timer_mu_;
  absl::optional<grpc_event_engine::experimental::EventEngine::TaskHandle>
      timer_handle_ ABSL_GUARDED_BY(&timer_mu_);
  bool cancelled_ ABSL_GUARDED_BY(&timer_mu_) = false;

  grpc_core::Duration report_interval_;
  ByteBuffer response_;
  std::shared_ptr<ReactorHook> hook_ = nullptr;
  std::shared_ptr<grpc_event_engine::experimental::EventEngine> engine_;
};

}  // namespace experimental
}  // namespace grpc

#endif  // GRPC_SRC_CPP_SERVER_ORCA_ORCA_SERVICE_H
