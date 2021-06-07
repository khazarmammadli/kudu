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

#include <string>

#include <oatpp/web/server/HttpRouter.hpp>

namespace oatpp {
namespace network {
class Server;

namespace tcp::server {
class ConnectionProvider;
}
}  // namespace network

namespace web::server {
class HttpConnectionHandler;
}

namespace swagger {
class Controller;
class Resources;
}  // namespace swagger

namespace parser::json::mapping {
class ObjectMapper;
}
}  // namespace oatpp

namespace kudu {
namespace rest {

class Controller;

class RestServer {
 private:
  std::shared_ptr<Controller> controller_;
  std::shared_ptr<oatpp::swagger::Controller> swagger_controller_;
  std::shared_ptr<oatpp::parser::json::mapping::ObjectMapper> object_mapper_;
  std::shared_ptr<oatpp::web::server::HttpRouter> router_;
  std::shared_ptr<oatpp::network::tcp::server::ConnectionProvider> connection_provider_;
  std::shared_ptr<oatpp::web::server::HttpConnectionHandler> connection_handler_;
  std::shared_ptr<oatpp::network::Server> server_;

  void Initialize(const std::string& master_addresses,
                  const std::string& rest_bind_address,
                  bool swagger_enabled);

  static void ParseBindAddress(const std::string& rest_bind_address,
                               std::string* host,
                               unsigned int* port);

 public:
  void Run();

  void Stop();

  explicit RestServer(const std::string& master_addresses,
                      const std::string& rest_bind_address,
                      bool swagger_enabled);
};

}  // namespace rest
}  // namespace kudu
