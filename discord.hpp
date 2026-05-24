#pragma once

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>
#include <string>
#include <vector>
#include <deque>
#include <atomic>
#include <mutex>
#include <thread>
#include <chrono>
#include <functional>
#include <memory>
#include <algorithm>
#include <cstring>
#include <iostream>
#include "third-party/json.hpp"
constexpr uint32_t MAX_RPC_PAYLOAD = 16384;
constexpr uint32_t STREAM_BUFFER_SIZE = 65536;
constexpr uint64_t WATCHDOG_TIMEOUT_MS = 30000;
constexpr size_t MAX_TX_QUEUE_SIZE = 64;
constexpr uint32_t WRITE_TOTAL_TIMEOUT_MS = 5000;
enum class ClientState : int { 
    Disconnected, 
    Connecting, 
    Connected, 
    Stopping 
};
enum class Opcode : uint32_t { 
    Handshake = 0, 
    Frame = 1, 
    Close = 2, 
    Ping = 3, 
    Pong = 4 
};
#pragma pack(push, 1)
struct MessageHeader { 
    uint32_t opcode; 
    uint32_t length; 
};
#pragma pack(pop)
struct SafeHandle {
    HANDLE handle = INVALID_HANDLE_VALUE;
    SafeHandle() = default;
    ~SafeHandle() { 
        close(); 
    }
    void close() { 
        if (
            handle != INVALID_HANDLE_VALUE && handle != NULL
        ) 
        { 
            ::CloseHandle(handle); handle = INVALID_HANDLE_VALUE; 
        } 
    }
    HANDLE get() 
    const { 
        return handle; 
    }
    HANDLE release() 
    noexcept { 
        HANDLE h = handle; 
        handle = INVALID_HANDLE_VALUE; 
        return h; 
    }
    void reset(HANDLE h) { 
        close(); 
        handle = h; 
    }
    bool isValid() 
    const { 
        return handle != INVALID_HANDLE_VALUE && handle != NULL; 
    }
};
struct SafeEvent {
    HANDLE hEvent = NULL;
    SafeEvent() = default;
    ~SafeEvent() { 
        close(); 
    }
    void close() { 
        if (hEvent) { 
            ::CloseHandle(hEvent); 
            hEvent = NULL; 
        } 
    }
    HANDLE get() const { 
        return hEvent; 
    }
    void create(BOOL manualReset, BOOL initialState) { 
        close(); 
        hEvent = ::CreateEventA(
            NULL, 
            manualReset, 
            initialState, 
            NULL
        ); 
    }
    bool isValid() const { 
        return hEvent != NULL; 
    }
};
struct DiscordButton { 
    std::string label; 
    std::string url; 
};
struct DiscordPresence {
    std::string state, 
    details, 
    largeImageKey, 
    largeImageText, 
    smallImageKey, 
    smallImageText, 
    partyId, 
    matchSecret, 
    joinSecret;
    int partySize = 0, 
    partyMax = 0;
    std::vector<DiscordButton> buttons;
};

class DiscordRpcClient : public std::enable_shared_from_this<DiscordRpcClient> {
    struct PrivateConstructorToken { 
        explicit PrivateConstructorToken(int) {} 
    };

public:
    using EventCallback = std::function<void(const std::string& name, const nlohmann::json& data)>;
    static std::shared_ptr<DiscordRpcClient> create(const std::string& clientId) { 
        return std::make_shared<DiscordRpcClient>(PrivateConstructorToken{0}, clientId); 
    }

    DiscordRpcClient(
        PrivateConstructorToken, 
        const std::string& clientId)
        : m_clientId(clientId), 
        m_state(ClientState::Disconnected), 
        m_nonce(1), 
        m_hasPresence(false) {
        m_rxChunkBuffer.resize(4096);
        m_streamBuffer.resize(STREAM_BUFFER_SIZE);
        std::memset(&m_ioOverlap, 0, sizeof(OVERLAPPED));
    }

    ~DiscordRpcClient() { 
        stop(); 
    }

    void setOnEvent(EventCallback cb) { 
        std::lock_guard<std::mutex> l(m_eventMtx); 
        m_eventCallback = cb; 
    }

    void start() {
        if (m_state.exchange(ClientState::Connecting) == ClientState::Connecting) return;
        m_shutdownEvent.create(TRUE, FALSE);
        m_ioEvent.create(TRUE, FALSE);
        m_writeEvent.create(TRUE, FALSE);
        m_txEvent.create(FALSE, FALSE);
        m_ioThread = std::thread(&DiscordRpcClient::ioWorkerLoop, this);
    }

    void stop() {
        if (m_state.exchange(ClientState::Stopping) == ClientState::Stopping) return;
        if (m_shutdownEvent.isValid()) ::SetEvent(m_shutdownEvent.get());
        close_connection_internal();
        if (m_ioThread.joinable()) m_ioThread.join();
    }

    void updatePresence(const DiscordPresence& p) {
        {
            std::lock_guard<std::mutex> lock(m_presenceMtx);
            m_lastPresence = p;
            m_hasPresence = true;
        }
        if (m_state.load() == ClientState::Connected) {
            nlohmann::json payload;
            { 
                std::lock_guard<std::mutex> lock(m_presenceMtx); 
                payload = buildPresenceJson(p); 
            }
            enqueueOutgoingFrame(payload.dump());
        }
    }

private:
    std::string m_clientId;
    std::atomic<ClientState> m_state{ClientState::Disconnected};
    std::atomic<uint64_t> m_nonce;
    std::mutex m_pipeMtx, m_presenceMtx, m_txMtx, m_eventMtx;
    std::deque<std::string> m_txQueue;
    EventCallback m_eventCallback;
    std::thread m_ioThread;
    SafeHandle m_pipeHandle;
    OVERLAPPED m_ioOverlap{};
    SafeEvent m_shutdownEvent, m_ioEvent, m_writeEvent, m_txEvent;
    std::vector<uint8_t> m_rxChunkBuffer, m_streamBuffer;
    size_t m_streamBytesValid = 0;
    std::atomic<bool> m_asyncReadPending{false};
    DiscordPresence m_lastPresence;
    bool m_hasPresence;

    nlohmann::json buildPresenceJson(const DiscordPresence& p) {
        nlohmann::json activity = {
            {"state", p.state}, 
            {"details", p.details}, 
            {"timestamps", {
                {"start", std::chrono::system_clock::now().time_since_epoch().count() / 1000000000}
            }
        }
    };
        nlohmann::json assets = nlohmann::json::object();
        if (!p.largeImageKey.empty()) assets["large_image"] = p.largeImageKey;
        if (!p.largeImageText.empty()) assets["large_text"] = p.largeImageText;
        if (!p.smallImageKey.empty()) assets["small_image"] = p.smallImageKey;
        if (!assets.empty()) activity["assets"] = assets;
        return {
            {"cmd", "SET_ACTIVITY"}, 
            {"args", {
                {"pid", (uint64_t)::GetCurrentProcessId()}, 
                {"activity", activity}
            }
        }, 
        {"nonce", std::to_string(m_nonce.fetch_add(1))}};
    }

    void enqueueOutgoingFrame(const std::string& payload, Opcode op = Opcode::Frame) {
        MessageHeader h{static_cast<uint32_t>(op), 
            (uint32_t)payload.size()};
        std::string frame; 
        frame.reserve(sizeof(h) + payload.size());
        frame.append((char*)&h, sizeof(h)); 
        frame.append(payload);
        { 
            std::lock_guard<std::mutex> l(m_txMtx); 
            m_txQueue.push_back(std::move(frame)); 
        }
        ::SetEvent(m_txEvent.get());
    }

    void ioWorkerLoop() {
        uint32_t delay = 1000;
        while (m_state.load() != ClientState::Stopping) {
            if (m_state.load() != ClientState::Connected) {
                std::unique_lock<std::mutex> ioLock(m_pipeMtx);
                if (tryConnectInternal_nolock()) {
                    if (sendHandshakeInternal_nolock(ioLock)) {
                        m_state.store(ClientState::Connected);
                        if (m_hasPresence) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(500));
                            nlohmann::json payload;
                            { 
                                std::lock_guard<std::mutex> l(m_presenceMtx); 
                                payload = buildPresenceJson(m_lastPresence); 
                            }
                            enqueueOutgoingFrame(payload.dump());
                        }
                        delay = 1000;
                    }
                } else {
                    ioLock.unlock();
                    if (::WaitForSingleObject(m_shutdownEvent.get(), delay) == WAIT_OBJECT_0) break;
                    delay = std::min(delay * 2, 10000u);
                }
            } else {
                handleWindowsIO();
            }
        }
    }

    void handleWindowsIO() {
        std::unique_lock<std::mutex> ioLock(m_pipeMtx);
        if (!m_pipeHandle.isValid()) return;
        if (!m_asyncReadPending.load()) {
            ::ResetEvent(m_ioEvent.get());
            std::memset(&m_ioOverlap, 0, sizeof(OVERLAPPED));
            m_ioOverlap.hEvent = m_ioEvent.get();
            m_asyncReadPending.store(true);
            DWORD bytes = 0;
            if (!::ReadFile(m_pipeHandle.get(), m_rxChunkBuffer.data(), 
            (DWORD)m_rxChunkBuffer.size(), 
            &bytes, 
            &m_ioOverlap) && ::GetLastError() != ERROR_IO_PENDING) {
                close_connection_internal_nolock(); 
                return;
            }
        }
        std::string frame;
        { 
            std::lock_guard<std::mutex> l(m_txMtx); 
            if (!m_txQueue.empty()) { 
            frame = std::move(m_txQueue.front()); 
            m_txQueue.pop_front(); 
        } 
    }
        if (!frame.empty() && !writeFrame(ioLock, frame)) { 
            close_connection_internal_nolock(); 
            return; 
        }
        HANDLE hs[] = { 
            m_shutdownEvent.get(), 
            m_ioEvent.get(), 
            m_txEvent.get() 
        };
        ioLock.unlock();
        DWORD res = ::WaitForMultipleObjects(3, hs, FALSE, 1000);
        ioLock.lock();
        if (res == WAIT_OBJECT_0 + 1) {
            DWORD transferred = 0;
            if (::GetOverlappedResult(m_pipeHandle.get(), &m_ioOverlap, &transferred, FALSE)) {
                processIncomingRawBytes(transferred);
            } else { 
                close_connection_internal_nolock(); 
            }
        }
    }

    bool writeFrame(std::unique_lock<std::mutex>& lock, const std::string& frame) {
        DWORD chunk = 0; OVERLAPPED ov{}; ov.hEvent = m_writeEvent.get();
        return ::WriteFile(m_pipeHandle.get(), frame.data(), (DWORD)frame.size(), &chunk, &ov) || ::GetLastError() == ERROR_IO_PENDING;
    }

    void processIncomingRawBytes(DWORD transferred) {
        m_streamBytesValid += transferred;
        size_t cursor = 0;
        while (m_streamBytesValid - cursor >= sizeof(MessageHeader)) {
            MessageHeader h; std::memcpy(&h, m_streamBuffer.data() + cursor, sizeof(h));
            if (m_streamBytesValid - cursor < sizeof(h) + h.length) break;
            const char* ptr = reinterpret_cast<const char*>(m_streamBuffer.data() + cursor + sizeof(h));
            if (h.opcode == (uint32_t)Opcode::Frame) {
                auto data = nlohmann::json::parse(std::string(ptr, h.length));
                std::lock_guard<std::mutex> el(m_eventMtx);
                if (m_eventCallback) m_eventCallback(data.value("evt", "unknown"), data.value("data", nlohmann::json::object()));
            }
            cursor += sizeof(h) + h.length;
        }
        m_asyncReadPending.store(false);
    }

    bool tryConnectInternal_nolock() {
        for (int i = 0; i < 10; ++i) {
            HANDLE h = ::CreateFileA(("\\\\.\\pipe\\discord-ipc-" + std::to_string(i)).c_str(), 
            GENERIC_READ|GENERIC_WRITE, 
            0, 
            NULL, 
            OPEN_EXISTING, 
            0, 
            NULL
        );
            if (h != INVALID_HANDLE_VALUE) { 
                m_pipeHandle.reset(h); 
                return true; 
            }
        }
        return false;
    }

    bool sendHandshakeInternal_nolock(std::unique_lock<std::mutex>& l) {
        std::string p = "{\"v\":1,\"client_id\":\"" + m_clientId + "\"}";
        MessageHeader h{0, (uint32_t)p.size()};
        std::string f; 
        f.append((char*)&h, sizeof(h)); 
        f.append(p);
        DWORD written;
        return ::WriteFile(
            m_pipeHandle.get(), 
            f.data(), 
            (DWORD)f.size(), 
            &written, 
            NULL
        );
    }

    void close_connection_internal() { 
        std::lock_guard<std::mutex> io(m_pipeMtx); 
        close_connection_internal_nolock(); 
    }
    void close_connection_internal_nolock() { 
        m_pipeHandle.close(); 
        m_state.store(ClientState::Disconnected); 
        m_asyncReadPending.store(false); 
    }
};