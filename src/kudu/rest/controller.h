// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

#include <exception>
#include <string>
#include <vector>
#include <memory>

#include <oatpp/codegen/api_controller/base_define.hpp>
#include <oatpp/core/Types.hpp>
#include <oatpp/core/async/Coroutine.hpp>
#include <oatpp/core/data/mapping/type/Object.hpp>
#include <oatpp/core/data/mapping/type/Primitive.hpp>
#include <oatpp/core/data/mapping/type/Vector.hpp>
#include <oatpp/core/macro/basic.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/web/protocol/http/Http.hpp>
#include <oatpp/web/protocol/http/incoming/Request.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <oatpp/web/server/api/Endpoint.hpp>

#include "kudu/client/schema.h"
#include "kudu/client/shared_ptr.h"  // IWYU pragma: keep
#include "kudu/util/status.h"
#include "kudu/rest/dto/table_schema_dto.h"
#include "kudu/rest/dto/alter_table_dto.h"
#include "kudu/rest/dto/status_dto.h"  // IWYU pragma: keep

namespace oatpp::parser::json::mapping {
class ObjectMapper;
}

namespace kudu {

namespace client {
class KuduClient;
}

namespace rest {

class ColumnDTO;

// APIController responsible for mapping Endpoints
#include OATPP_CODEGEN_BEGIN(ApiController)  // Begin Codegen // IWYU pragma: keep

class Controller : public oatpp::web::server::api::ApiController {
 private:
  std::vector<std::string> master_addresses_;

 public:
  Controller(const std::shared_ptr<oatpp::parser::json::mapping::ObjectMapper>& object_mapper,
             const std::string& master_addresses);

  ENDPOINT_INFO(KuduTableSchema) {
    info->summary = "Get a Kudu Table Schema";
    info->addResponse<Object<TableSchemaDTO>>(Status::CODE_200, "application/json");
    info->addResponse<Object<StatusDTO>>(Status::CODE_400, "application/json");
    info->pathParams["tableName"].description = "Table name to be provided";
  }
  ENDPOINT("GET", "/GetKuduTableSchema/{tableName}", KuduTableSchema, PATH(String, tableName));

  ENDPOINT_INFO(CreateKuduTable) {
    info->summary = "Create a new Kudu Table";
    info->addConsumes<Object<TableSchemaDTO>>("application/json");
    info->addResponse<Object<StatusDTO>>(Status::CODE_200, "application/json");
    info->addResponse<Object<StatusDTO>>(Status::CODE_400, "application/json");
  }
  ENDPOINT("POST",
           "/CreateKuduTable",
           CreateKuduTable,
           BODY_DTO(Object<TableSchemaDTO>, kuduTableSchemaDTO));

  ENDPOINT_INFO(RenameTable) {
    info->summary = "Rename a Kudu table";
    info->addConsumes<Object<AlterTableDTO>>("application/json");
    info->addResponse<Object<StatusDTO>>(Status::CODE_200, "application/json");
    info->addResponse<Object<StatusDTO>>(Status::CODE_400, "application/json");
  }
  ENDPOINT("PUT", "/RenameTable", RenameTable, BODY_DTO(Object<AlterTableDTO>, alterTableDTO));

  ENDPOINT_INFO(DeleteKuduTable) {
    info->summary = "Delete a Kudu Table";
    info->addResponse<Object<StatusDTO>>(Status::CODE_200, "application/json");
    info->addResponse<Object<StatusDTO>>(Status::CODE_400, "application/json");
    info->pathParams["tableName"].description = "Name of the table to be deleted";
  }
  ENDPOINT("DELETE", "/DeleteKuduTable/{tableName}", DeleteKuduTable, PATH(String, tableName));

 private:
  kudu::Status GetTableSchema(const String& table_name,
                              client::KuduSchema* kudu_schema,
                              bool* exists);

  kudu::Status CreateKuduTable(const Object<TableSchemaDTO>& kudu_table_schema_dto, bool* exists);

  kudu::Status AlterTableName(const Object<AlterTableDTO>& alter_table_dto);

  kudu::Status DeleteKuduTable(const String& table_name, bool* exists);

  static oatpp::data::mapping::type::Vector<Object<ColumnDTO>> GetKuduColumnsAsOatppVector(
      const client::KuduSchema& kudu_schema);

  static kudu::Status CreateClient(const std::vector<std::string>& master_addrs,
                                   client::sp::shared_ptr<client::KuduClient>* client);

  static kudu::Status CreateTable(const client::sp::shared_ptr<client::KuduClient>& client,
                                  const std::string& table_name,
                                  const client::KuduSchema& schema,
                                  const std::vector<std::string>& partition_columns);

  static kudu::client::KuduSchema CreateSchema(const Object<TableSchemaDTO>& kudu_table_schema_dto,
                                               std::vector<std::string>* partition_columns);

  static kudu::Status DoesTableExist(const client::sp::shared_ptr<client::KuduClient>& client,
                                     const std::string& table_name,
                                     bool* exists);

  static kudu::Status AlterTable(const client::sp::shared_ptr<client::KuduClient>& client,
                                 const std::string& old_table_name,
                                 const std::string& new_table_name);

  static std::vector<std::string> ParseString(const std::string& comma_sep_addrs);
};
#include OATPP_CODEGEN_END(ApiController)  // End Codegen // IWYU pragma: keep

}  // namespace rest
}  // namespace kudu
