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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"

#include "kudu/client/client.h"
#include "kudu/client/schema.h"
#include "kudu/integration-tests/external_mini_cluster-itest-base.h"
#include "kudu/mini-cluster/external_mini_cluster.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/net/net_util.h"
#include "kudu/util/easy_json.h"

using kudu::ExternalMiniClusterITestBase;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduTableCreator;
using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::StringBuffer;
using rapidjson::Value;
using std::string;
using std::vector;

namespace kudu {
class RestServerITest : public ExternalMiniClusterITestBase {
  void SetUp() override {
    host_ = GetBindIpForDaemon(1, BindMode::WILDCARD);
    port_ = 0;
    ASSERT_OK(GetRandomPort(host_, &port_));
    url_ = "http://" + host_ + ":" + std::to_string(port_) + "/";
    string rest_address = "--rest_server_bind_address=" + host_ + ":" + std::to_string(port_);

    vector<string> flags = {"--rest_server_enabled=true",
                            "--rest_swagger_enabled=false",
                            rest_address};
    kudu::cluster::ExternalMiniClusterOptions opts;
    opts.num_masters = 1;
    opts.num_tablet_servers = 3;
    opts.extra_master_flags = std::move(flags);
    StartClusterWithOpts(std::move(opts));

    KuduSchema schema;
    KuduSchemaBuilder b;
    b.AddColumn("key")->Type(KuduColumnSchema::INT32)->NotNull()->PrimaryKey();
    b.AddColumn("int_val")->Type(KuduColumnSchema::INT32)->NotNull();
    ASSERT_OK(b.Build(&schema));
    std::shared_ptr<KuduTableCreator> table_creator(client_->NewTableCreator());
    vector<string> column_names = {"key"};
    table_creator->table_name("table_test")
        .schema(&schema)
        .set_range_partition_columns(column_names);
    ASSERT_OK(table_creator->Create());
  }

  void TearDown() override { ExternalMiniClusterITestBase::TearDown(); }

 public:
  string host_;
  uint16_t port_;
  string url_;
};

TEST_F(RestServerITest, TestCreateTable) {
  EasyCurl curl;
  EasyJson table_schema;
  EasyJson kudu_key_column;
  EasyJson kudu_data_column;

  table_schema["tableName"] = "table_new";
  kudu_key_column = table_schema["kuduColumns"].PushBack(EasyJson::kObject);
  kudu_data_column = table_schema["kuduColumns"].PushBack(EasyJson::kObject);

  kudu_key_column["columnName"] = "key";
  kudu_key_column["columnDataType"] = "INT32";
  kudu_key_column["isPrimaryKey"] = true;
  kudu_key_column["isNullable"] = false;

  kudu_data_column["columnName"] = "integer_val";
  kudu_data_column["columnDataType"] = "INT32";
  kudu_data_column["isPrimaryKey"] = false;
  kudu_data_column["isNullable"] = false;

  string url = url_ + "CreateKuduTable";
  string data = table_schema.ToString();
  faststring response;
  ASSERT_OK(curl.PostToURL(url, data, &response));

  Document json;
  json.Parse(response.ToString().c_str());
  string status = json["status"].GetString();
  int status_code = json["code"].GetInt();
  string status_message = json["message"].GetString();

  ASSERT_EQ("OK", status);
  ASSERT_EQ(200, status_code);
  ASSERT_EQ("Table has been created", status_message);

  faststring fail_response;
  curl.PostToURL(url, data, &fail_response);
  json.RemoveAllMembers();
  json.Parse(fail_response.ToString().c_str());

  status = json["status"].GetString();
  status_code = json["code"].GetInt();
  status_message = json["message"].GetString();

  ASSERT_EQ("Bad Request", status);
  ASSERT_EQ(400, status_code);
  ASSERT_EQ("Table either exists or client side error", status_message);
}

TEST_F(RestServerITest, TestGetTableSchema) {
  EasyCurl curl;
  string table_name = "table_test";
  string url = url_ + "GetKuduTableSchema/" + table_name;
  faststring response;
  curl.set_custom_method("GET");
  ASSERT_OK(curl.FetchURL(url, &response));

  Document json;
  json.Parse(response.ToString().c_str());
  string expected_table_name = json["tableName"].GetString();
  const Value& kudu_columns = json["kuduColumns"];

  string key_column_name = kudu_columns[0]["columnName"].GetString();
  string key_column_data_type = kudu_columns[0]["columnDataType"].GetString();
  bool key_primary_key = kudu_columns[0]["isPrimaryKey"].GetBool();
  bool key_nullable = kudu_columns[0]["isNullable"].GetBool();

  string data_column_name = kudu_columns[1]["columnName"].GetString();
  string data_column_data_type = kudu_columns[1]["columnDataType"].GetString();
  bool data_primary_key = kudu_columns[1]["isPrimaryKey"].GetBool();
  bool data_nullable = kudu_columns[1]["isNullable"].GetBool();

  ASSERT_EQ("table_test", expected_table_name);
  ASSERT_EQ("key", key_column_name);
  ASSERT_EQ("INT32", key_column_data_type);
  ASSERT_EQ(true, key_primary_key);
  ASSERT_EQ(false, key_nullable);
  ASSERT_EQ("int_val", data_column_name);
  ASSERT_EQ("INT32", data_column_data_type);
  ASSERT_EQ(false, data_primary_key);
  ASSERT_EQ(false, data_nullable);
}

TEST_F(RestServerITest, TestDeleteTable) {
  EasyCurl curl;
  string table_name = "table_test";
  string url = url_ + "DeleteKuduTable/" + table_name;
  faststring response;
  curl.set_custom_method("DELETE");
  ASSERT_OK(curl.FetchURL(url, &response));

  Document json;
  json.Parse(response.ToString().c_str());
  string status = json["status"].GetString();
  int status_code = json["code"].GetInt();
  string status_message = json["message"].GetString();

  ASSERT_EQ("OK", status);
  ASSERT_EQ(200, status_code);
  ASSERT_EQ("Table has been deleted", status_message);

  faststring fail_response;
  curl.FetchURL(url, &fail_response);
  json.RemoveAllMembers();
  json.Parse(fail_response.ToString().c_str());

  status = json["status"].GetString();
  status_code = json["code"].GetInt();
  status_message = json["message"].GetString();

  ASSERT_EQ("Bad Request", status);
  ASSERT_EQ(400, status_code);
  ASSERT_EQ("Table was not deleted, either it doesn't exists or client error", status_message);
}

TEST_F(RestServerITest, TestRenameTable) {
  EasyCurl curl;
  string url = url_ + "/" + "RenameTable";
  string data = "{\"oldTableName\":\"table_test\",\"newTableName\":\"new_table_test\"}";
  faststring response;
  curl.set_custom_method("PUT");
  ASSERT_OK(curl.PostToURL(url, data, &response));
  Document json;
  json.Parse(response.ToString().c_str());
  string status = json["status"].GetString();
  int status_code = json["code"].GetInt();
  string status_message = json["message"].GetString();

  ASSERT_EQ("OK", status);
  ASSERT_EQ(200, status_code);
  ASSERT_EQ("Table name has been changed from table_test to new_table_test", status_message);

  faststring fail_response;
  curl.set_custom_method("PUT");
  curl.PostToURL(url, data, &fail_response);
  json.RemoveAllMembers();
  json.Parse(fail_response.ToString().c_str());

  status = json["status"].GetString();
  status_code = json["code"].GetInt();
  status_message = json["message"].GetString();

  ASSERT_EQ("Bad Request", status);
  ASSERT_EQ(400, status_code);
  ASSERT_EQ("Unsuccessful, either no table with such name exists or a client side error",
            status_message);

  string retest_url = url_ + "GetKuduTableSchema/new_table_test";
  faststring retest_response;
  curl.set_custom_method("GET");
  ASSERT_OK(curl.FetchURL(retest_url, &retest_response));
}

}  // namespace kudu
