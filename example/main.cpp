#include "EverRPC/discord.hpp"
#include <iostream>

int main() {
    const std::string myClientId = "123456789012345678"; 
    auto client = DiscordRpcClient::create(myClientId);

    client->setOnEvent([](const std::string& name, const nlohmann::json& data) {
        std::cout << "[Debug] " << name << std::endl;
    });

    client->start();

    DiscordPresence p;
    p.state = "your state";
    p.details = "your details";
    p.largeImageKey = "your key assert";

    std::cout << "Presencia configurada. Esperando a que el Pipe esté libre..." << std::endl;
    client->updatePresence(p);

    std::cin.get();
    client->stop();
    return 0;
}