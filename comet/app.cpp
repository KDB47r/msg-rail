#include "app.h"

#include <grpcpp/grpcpp.h>

#include <thread>

#include "comet_grpc_service.h"
#include "comet_server.h"
#include "logging.h"
#include "signal_handler.h"

namespace sparkpush {

// 创建并启动 Comet 的 WebSocket 与 gRPC 服务。
void RunComet(const Config& cfg) {
    CometServer server(cfg);
    server.SetThreadNum(cfg.comet_io_threads);
    server.Start();
    LogInfo("Comet server listening on port " +
            std::to_string(cfg.listen_port));

    std::string addr =
        cfg.listen_addr + ":" + std::to_string(cfg.comet_grpc_port);

    //grpc
    grpc::ServerBuilder builder;
    CometServiceImpl grpcService(&server);
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&grpcService);
    std::unique_ptr<grpc::Server> grpcServer = builder.BuildAndStart();
    LogInfo("Comet gRPC server listening on " + addr);

    //kill/ctrl+C 优雅关闭
    InstallSignalHandler([&server, &grpcServer]() {
        server.Stop();
        if (grpcServer) grpcServer->Shutdown();
    });

    std::thread grpc_thread([&grpcServer]() { grpcServer->Wait(); });


    //  *** 
    // 阻塞在 asio io_context 上，直到 Stop() 被触发。
    server.Run();

    LogInfo("Comet shutting down gracefully...");
    server.Stop();
    if (grpcServer) grpcServer->Shutdown();
    if (grpc_thread.joinable()) grpc_thread.join();
    LogInfo("Comet shutdown complete");
}

}  // namespace sparkpush

//
