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
#include "kudu/rest/rest_server.h"

#include <vector>
#include <utility>

#include <oatpp/network/Server.hpp>
#include <oatpp/network/tcp/server/ConnectionProvider.hpp>
#include <oatpp/web/server/HttpConnectionHandler.hpp>
#include <oatpp/web/server/HttpRouter.hpp>
#include <oatpp-swagger/Controller.hpp>
#include <oatpp-swagger/Resources.hpp>

#include "kudu/rest/controller.h"
#include "kudu/util/version_info.h"
#include "kudu/gutil/strings/split.h"
#include "kudu/gutil/strings/numbers.h"

using oatpp::String;
using oatpp::UInt16;
using oatpp::network::Address;
using oatpp::network::Server;
using oatpp::network::tcp::server::ConnectionProvider;
using oatpp::parser::json::mapping::Deserializer;
using oatpp::parser::json::mapping::ObjectMapper;
using oatpp::parser::json::mapping::Serializer;
using oatpp::swagger::DocumentInfo;
using oatpp::swagger::Resources;
using oatpp::web::server::HttpConnectionHandler;

using std::string;

namespace kudu {
namespace rest {

void RestServer::Initialize(const string& master_addresses,
                            const string& rest_bind_address,
                            bool swagger_enabled) {
  string host;
  unsigned int port;
  string rest_url = "http://" + rest_bind_address;
  String rest_url_oatpp = rest_url.data();
  String rest_server_description = "server on " + rest_url_oatpp;
  ParseBindAddress(rest_bind_address, &host, &port);
  oatpp::String temp_host = host.data();
  oatpp::UInt16 temp_port = port;

  router_ = oatpp::web::server::HttpRouter::createShared();

  auto serialize_config = Serializer::Config::createShared();
  auto deserialize_config = Deserializer::Config::createShared();
  serialize_config->useBeautifier = true;
  object_mapper_ = ObjectMapper::createShared(serialize_config, deserialize_config);

  controller_ = std::make_shared<Controller>(object_mapper_, master_addresses);
  controller_->addEndpointsToRouter(router_);

  if (swagger_enabled) {
    auto swaggerResources = Resources::loadResources(OATPP_SWAGGER_RES_PATH);
    DocumentInfo::Builder builder;
    string version_info = VersionInfo::GetShortVersionInfo();
    builder.setTitle("Kudu REST Service with Swagger-UI")
        .setDescription("C++/Oat++ Web Service with Swagger-UI")
        .setVersion(version_info.data())
        .setContactName("Apache Kudu")
        .setContactUrl("https://kudu.apache.org/")
        .setLicenseName("Apache License, Version 2.0")
        .setLicenseUrl("http://www.apache.org/licenses/LICENSE-2.0")
        .addServer(rest_url_oatpp, rest_server_description);
    auto documentInfo = builder.build();
    auto docEndpoints = oatpp::swagger::Controller::Endpoints::createShared();
    docEndpoints->pushBackAll(controller_->getEndpoints());
    swagger_controller_ =
        oatpp::swagger::Controller::createShared(docEndpoints, documentInfo, swaggerResources);
    swagger_controller_->addEndpointsToRouter(router_);
  }
  connection_provider_ = ConnectionProvider::createShared({temp_host, temp_port, Address::IP_4});
  connection_handler_ = HttpConnectionHandler::createShared(router_);

  server_ = Server::createShared(connection_provider_, connection_handler_);
}

void RestServer::Run() { server_->run(); }

void RestServer::Stop() {
  if (server_->getStatus() == Server::STATUS_RUNNING) {
    server_->stop();
  }
  connection_provider_->stop();

  connection_handler_->stop();
}

RestServer::RestServer(const string& master_addresses,
                       const string& rest_bind_address,
                       bool swagger_enabled) {
  Initialize(master_addresses, rest_bind_address, swagger_enabled);
}

void RestServer::ParseBindAddress(const string& rest_bind_address,
                                  string* host,
                                  unsigned int* port) {
  std::pair<string, string> p =
      strings::Split(rest_bind_address, strings::delimiter::Limit(":", 1));
  StripWhiteSpace(&p.first);
  unsigned int port_num = 8061;
  *host = p.first;
  if (p.second.empty()) {
    *port = 8061;
  } else if (SimpleAtoi(p.second, &port_num)) {
    *port = port_num;
  }
}

}  // namespace rest
}  // namespace kudu
