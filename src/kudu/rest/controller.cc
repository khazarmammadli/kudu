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
#include "kudu/rest/controller.h"

#include <algorithm>

#include <glog/logging.h>

#include "kudu/client/client.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/util/monotime.h"
#include "kudu/rest/dto/column_dto.h"

#include <oatpp/core/base/StrBuffer.hpp>
#include <oatpp/core/data/mapping/type/Type.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>  // IWYU pragma: keep

using kudu::MonoDelta;
using kudu::Status;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::sp::shared_ptr;

using oatpp::Object;
using oatpp::String;
using oatpp::data::mapping::type::Vector;
using oatpp::web::protocol::http::outgoing::Response;
using oatpp::web::server::api::ApiController;

using std::string;
using std::vector;

namespace oatpp::web::protocol::http::outgoing {
class Response;
}

namespace kudu {
namespace rest {

Controller::Controller(
    const std::shared_ptr<oatpp::parser::json::mapping::ObjectMapper>& object_mapper,
    const string& master_addresses)
    : ApiController(object_mapper), master_addresses_(ParseString(master_addresses)) {}

std::shared_ptr<Response> Controller::KuduTableSchema(const String& table_name) {
  KuduSchema kudu_schema;
  bool exists = false;
  kudu::Status s = GetTableSchema(table_name, &kudu_schema, &exists);

  if (s.ok()) {
    auto kudu_table_schema_dto = TableSchemaDTO::createShared();
    kudu_table_schema_dto->kuduColumns = {};
    kudu_table_schema_dto->tableName = table_name;
    kudu_table_schema_dto->kuduColumns = GetKuduColumnsAsOatppVector(kudu_schema);
    return createDtoResponse(Status::CODE_200, kudu_table_schema_dto);
  }
  auto status_dto = StatusDTO::createShared();
  status_dto->code = 400;
  status_dto->status = "";
  status_dto->message = "Couldn't find a table with the provided name: " + table_name;
  return createDtoResponse(Status::CODE_400, status_dto);
}

std::shared_ptr<Response> Controller::CreateKuduTable(
    const Object<TableSchemaDTO>& kudu_table_schema_dto) {
  bool exists = false;
  auto status_dto = StatusDTO::createShared();

  kudu::Status s = CreateKuduTable(kudu_table_schema_dto, &exists);

  if (s.ok()) {
    status_dto->code = 200;
    status_dto->status = "OK";
    status_dto->message = "Table has been created";
    return createDtoResponse(Status::CODE_200, status_dto);
  }
  status_dto->code = 400;
  status_dto->status = "Bad Request";
  status_dto->message = "Table either exists or client side error";
  return createDtoResponse(Status::CODE_400, status_dto);
}

std::shared_ptr<Response> Controller::RenameTable(const Object<AlterTableDTO>& alter_table_dto) {
  kudu::Status s = AlterTableName(alter_table_dto);
  auto status_dto = StatusDTO::createShared();
  if (s.ok()) {
    status_dto->code = 200;
    status_dto->status = "OK";
    string message = "Table name has been changed from " +
                     alter_table_dto->oldTableName->std_str() + " to " +
                     alter_table_dto->newTableName->std_str();
    status_dto->message = message.data();
    return createDtoResponse(Status::CODE_200, status_dto);
  }
  status_dto->code = 400;
  status_dto->status = "Bad Request";
  status_dto->message =
      "Unsuccessful, either no table with such name exists or a client side error";
  return createDtoResponse(Status::CODE_400, status_dto);
}

std::shared_ptr<Response> Controller::DeleteKuduTable(const String& table_name) {
  bool exists = false;
  kudu::Status s = DeleteKuduTable(table_name, &exists);
  auto status_dto = StatusDTO::createShared();

  if (s.ok()) {
    status_dto->code = 200;
    status_dto->status = "OK";
    status_dto->message = "Table has been deleted";
    return createDtoResponse(Status::CODE_200, status_dto);
  }
  status_dto->code = 400;
  status_dto->status = "Bad Request";
  status_dto->message = "Table was not deleted, either it doesn't exists or client error";
  return createDtoResponse(Status::CODE_400, status_dto);
}

Status Controller::GetTableSchema(const String& table_name, KuduSchema* kudu_schema, bool* exists) {
  shared_ptr<KuduClient> kudu_client;
  RETURN_NOT_OK(CreateClient(master_addresses_, &kudu_client));
  RETURN_NOT_OK(DoesTableExist(kudu_client, table_name->std_str(), exists));
  if (*exists) {
    RETURN_NOT_OK(kudu_client->GetTableSchema(table_name->std_str(), kudu_schema));
  }
  return kudu::Status::OK();
}

Status Controller::CreateKuduTable(const Object<TableSchemaDTO>& kudu_table_schema_dto,
                                   bool* exists) {
  shared_ptr<KuduClient> kudu_client;
  RETURN_NOT_OK(CreateClient(master_addresses_, &kudu_client));
  vector<string> partition_columns;

  KuduSchema schema = CreateSchema(kudu_table_schema_dto, &partition_columns);
  RETURN_NOT_OK(DoesTableExist(kudu_client, kudu_table_schema_dto->tableName->std_str(), exists));

  if (!*exists) {
    RETURN_NOT_OK(CreateTable(
        kudu_client, kudu_table_schema_dto->tableName->std_str(), schema, partition_columns));
    return kudu::Status::OK();
  }
  // No Bad Request in status messages, as such substitute with not found
  return kudu::Status::NotFound("Table creation failed");
}

Status Controller::AlterTableName(const Object<AlterTableDTO>& alter_table_dto) {
  shared_ptr<KuduClient> kudu_client;
  RETURN_NOT_OK(CreateClient(master_addresses_, &kudu_client));
  kudu::Status s = AlterTable(kudu_client,
                              alter_table_dto->oldTableName->std_str(),
                              alter_table_dto->newTableName->std_str());
  return s;
}

Status Controller::DeleteKuduTable(const String& table_name, bool* exists) {
  shared_ptr<KuduClient> kudu_client;
  RETURN_NOT_OK(CreateClient(master_addresses_, &kudu_client));

  RETURN_NOT_OK(DoesTableExist(kudu_client, table_name->std_str(), exists));

  if (*exists) {
    RETURN_NOT_OK(kudu_client->DeleteTable(table_name->std_str()));
    return kudu::Status::OK();
  }

  return kudu::Status::NotFound("Table deletion failed");
}

Vector<Object<ColumnDTO>> Controller::GetKuduColumnsAsOatppVector(const KuduSchema& kudu_schema) {
  vector<int> primary_keys;
  bool isPrimaryKey = false;

  kudu_schema.GetPrimaryKeyColumnIndexes(&primary_keys);

  Vector<Object<ColumnDTO>> kudu_columns = Vector<Object<ColumnDTO>>::createShared();

  for (int i = 0; i < kudu_schema.num_columns(); i++) {
    KuduColumnSchema kudu_column = kudu_schema.Column(i);

    if (std::find(primary_keys.begin(), primary_keys.end(), i) != primary_keys.end()) {
      isPrimaryKey = true;
    }

    Object<ColumnDTO> kudu_column_dto = ColumnDTO::createShared();
    kudu_column_dto->columnName = kudu_column.name().data();
    kudu_column_dto->columnDataType = KuduColumnSchema::DataTypeToString(kudu_column.type()).data();
    kudu_column_dto->isPrimaryKey = isPrimaryKey;
    kudu_column_dto->isNullable = kudu_schema.Column(i).is_nullable();

    kudu_columns->push_back(kudu_column_dto);

    isPrimaryKey = false;
  }
  return kudu_columns;
}

Status Controller::CreateClient(const vector<string>& master_addrs,
                                client::sp::shared_ptr<client::KuduClient>* client) {
  return KuduClientBuilder()
      .master_server_addrs(master_addrs)
      .default_admin_operation_timeout(MonoDelta::FromSeconds(20))
      .Build(client);
}

Status Controller::CreateTable(const shared_ptr<KuduClient>& client,
                               const string& table_name,
                               const KuduSchema& schema,
                               const vector<string>& partition_columns) {
  std::unique_ptr<KuduTableCreator> table_creator(client->NewTableCreator());
  table_creator->table_name(table_name)
      .schema(&schema)
      .set_range_partition_columns(partition_columns);

  kudu::Status s = table_creator->Create();

  return s;
}

KuduSchema Controller::CreateSchema(const Object<TableSchemaDTO>& kudu_table_schema_dto,
                                    std::vector<std::string>* partition_columns) {
  KuduSchemaBuilder builder;
  KuduSchema schema;
  KuduColumnSchema::DataType data_type;

  for (const auto& kudu_column_dto : *kudu_table_schema_dto->kuduColumns) {
    KuduColumnSchema::StringToDataType(kudu_column_dto->columnDataType->std_str(), &data_type);
    auto column = builder.AddColumn(kudu_column_dto->columnName->std_str());
    if (kudu_column_dto->isPrimaryKey) {
      column->Type(data_type)->NotNull()->PrimaryKey();
      partition_columns->push_back(kudu_column_dto->columnName->std_str());
    }
    if (!kudu_column_dto->isPrimaryKey && kudu_column_dto->isNullable) {
      column->Type(data_type)->Nullable();
    }
    if (!kudu_column_dto->isPrimaryKey && !kudu_column_dto->isNullable) {
      column->Type(data_type)->NotNull();
    }
  }

  KUDU_CHECK_OK(builder.Build(&schema));

  return schema;
}

Status Controller::DoesTableExist(const shared_ptr<KuduClient>& client,
                                  const string& table_name,
                                  bool* exists) {
  shared_ptr<KuduTable> table;
  kudu::Status s = client->OpenTable(table_name, &table);
  if (s.ok()) {
    *exists = true;
  } else if (s.IsNotFound()) {
    *exists = false;
    s = kudu::Status::OK();
  }
  return s;
}

Status Controller::AlterTable(const shared_ptr<KuduClient>& client,
                              const string& old_table_name,
                              const string& new_table_name) {
  std::unique_ptr<KuduTableAlterer> table_alterer(client->NewTableAlterer(old_table_name));
  table_alterer->RenameTo(new_table_name);
  kudu::Status s = table_alterer->Alter();

  return s;
}

vector<string> Controller::ParseString(const string& comma_sep_addrs) {
  return strings::Split(comma_sep_addrs, ",", strings::SkipEmpty());
}

}  // namespace rest
}  // namespace kudu
