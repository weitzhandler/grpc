//
// Copyright 2022 gRPC authors.
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

#include "src/cpp/server/orca/orca_service.h"

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

#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "src/core/load_balancing/backend_metric_data.h"
#include "src/cpp/server/backend_metric_recorder.h"
#include "upb/base/string_view.h"
#include "upb/mem/arena.hpp"
#include "xds/data/orca/v3/orca_load_report.upb.h"

namespace grpc {
namespace experimental {

//
// OrcaService
//

OrcaService::OrcaService(ServerMetricRecorder* const server_metric_recorder,
                         Options options)
    : server_metric_recorder_(server_metric_recorder),
      min_report_duration_(options.min_report_duration) {
  CHECK_NE(server_metric_recorder_, nullptr);
  AddMethod(new internal::RpcServiceMethod(
      "/xds.service.orca.v3.OpenRcaService/StreamCoreMetrics",
      internal::RpcMethod::SERVER_STREAMING, /*handler=*/nullptr));
  MarkMethodCallback(
      0, new internal::CallbackServerStreamingHandler<ByteBuffer, ByteBuffer>(
             [this](CallbackServerContext* ctx, const ByteBuffer* request) {
               return new Reactor(this, ctx->peer(), request, nullptr);
             }));
}

Slice OrcaService::GetOrCreateSerializedResponse() {
  grpc::internal::MutexLock lock(&mu_);
  std::shared_ptr<const ServerMetricRecorder::BackendMetricDataState> result =
      server_metric_recorder_->GetMetricsIfChanged();
  if (!response_slice_seq_.has_value() ||
      *response_slice_seq_ != result->sequence_number) {
    const auto& data = result->data;
    upb::Arena arena;
    xds_data_orca_v3_OrcaLoadReport* response =
        xds_data_orca_v3_OrcaLoadReport_new(arena.ptr());
    if (data.cpu_utilization != -1) {
      xds_data_orca_v3_OrcaLoadReport_set_cpu_utilization(response,
                                                          data.cpu_utilization);
    }
    if (data.mem_utilization != -1) {
      xds_data_orca_v3_OrcaLoadReport_set_mem_utilization(response,
                                                          data.mem_utilization);
    }
    if (data.application_utilization != -1) {
      xds_data_orca_v3_OrcaLoadReport_set_application_utilization(
          response, data.application_utilization);
    }
    if (data.qps != -1) {
      xds_data_orca_v3_OrcaLoadReport_set_rps_fractional(response, data.qps);
    }
    if (data.eps != -1) {
      xds_data_orca_v3_OrcaLoadReport_set_eps(response, data.eps);
    }
    for (const auto& u : data.utilization) {
      xds_data_orca_v3_OrcaLoadReport_utilization_set(
          response,
          upb_StringView_FromDataAndSize(u.first.data(), u.first.size()),
          u.second, arena.ptr());
    }
    size_t buf_length;
    char* buf = xds_data_orca_v3_OrcaLoadReport_serialize(response, arena.ptr(),
                                                          &buf_length);
    response_slice_.emplace(buf, buf_length);
    response_slice_seq_ = result->sequence_number;
  }
  return Slice(*response_slice_);
}

}  // namespace experimental
}  // namespace grpc
