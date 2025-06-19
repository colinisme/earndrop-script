-- 优化版 wrk 压测脚本 - 专门处理大数据量CSV文件
-- 使用方法: wrk -t24 -c3000 -d60s -s load_test_wrk_post.lua -- https://earndrop.prd.galaxy.eco

-- 读取地址列表 - 优化版本
local addresses = {}
local address_index = 1
local sample_size = 10000  -- 从200万条数据中采样的大小
local error_count = 0  -- 全局错误计数器
local error_log_file = nil  -- 错误日志文件句柄

-- 从CSV文件读取地址 - 使用采样方式
local function load_addresses_sampled()
    local file = io.open("data3.csv", "r")
    if not file then
        print("Warning: Could not open data3.csv, using default addresses")
        return {
            "0x93Cb1a4A9Bdea09162548243fD298F57cFc27F70",
            "0x1d030d76f5a6505a64c032cf6b350c379d52ae60",
            "0x28b9EAfD6Ea52E56Ff4fE0a0223EDdc4bC3D361e"
        }
    end
    
    -- 先计算总行数
    local total_lines = 0
    for _ in file:lines() do
        total_lines = total_lines + 1
    end
    file:close()
    
    print("Total lines in CSV: " .. total_lines)
    
    -- 如果总行数小于采样大小，读取所有行
    if total_lines <= sample_size then
        print("Reading all " .. total_lines .. " addresses...")
        file = io.open("data3.csv", "r")
        for line in file:lines() do
            line = line:gsub("%s+", "")  -- 去除空白字符
            if line and line ~= "" then
                table.insert(addresses, line)
            end
        end
        file:close()
    else
        -- 随机采样
        print("Sampling " .. sample_size .. " addresses from " .. total_lines .. " total...")
        local sampled_indices = {}
        for i = 1, sample_size do
            sampled_indices[i] = math.random(1, total_lines)
        end
        
        -- 排序索引以提高读取效率
        table.sort(sampled_indices)
        
        file = io.open("data3.csv", "r")
        local current_line = 0
        local sample_index = 1
        
        for line in file:lines() do
            current_line = current_line + 1
            
            if sample_index <= #sampled_indices and current_line == sampled_indices[sample_index] then
                line = line:gsub("%s+", "")  -- 去除空白字符
                if line and line ~= "" then
                    table.insert(addresses, line)
                end
                sample_index = sample_index + 1
            end
            
            if sample_index > #sampled_indices then
                break
            end
        end
        file:close()
    end
    
    print("Loaded " .. #addresses .. " addresses (sampled from " .. total_lines .. " total)")
    
    -- 如果没有加载到地址，使用默认地址
    if #addresses == 0 then
        print("No addresses loaded, using default addresses")
        addresses = {
            "0x93Cb1a4A9Bdea09162548243fD298F57cFc27F70",
            "0x1d030d76f5a6505a64c032cf6b350c379d52ae60",
            "0x28b9EAfD6Ea52E56Ff4fE0a0223EDdc4bC3D361e"
        }
    end
    
    return addresses
end

-- 加载地址
addresses = load_addresses_sampled()

-- 请求函数 - 使用随机选择地址
function request()
    -- 随机选择一个地址
    local address = addresses[math.random(1, #addresses)]
    
    -- 设置headers
    wrk.headers["X-Address"] = address
    wrk.headers["Content-Type"] = "application/json"
    wrk.headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    

    local id = math.random(1, 1000)
    local body = string.format('{"id":%d, "key":"value"}', id)
    wrk.body = body
    
    wrk.method = "POST"

    -- 返回请求
    return wrk.format("POST", "/sahara/prepare_claim")
end

-- 响应处理（可选）
function response(status, headers, body)
    if status ~= 200 then
        error_count = error_count + 1  -- 增加错误计数
        
        local body_text = body or "No body"
        if type(body_text) == "table" then
            body_text = "Table body"
        elseif type(body_text) == "string" and #body_text > 500 then
            body_text = body_text:sub(1, 500) .. "..."
        end
        
        -- 构建详细的错误日志
        local timestamp = os.date("%Y-%m-%d %H:%M:%S")
        local error_type = ""
        if status >= 400 and status < 500 then
            error_type = "Client Error (4xx)"
        elseif status >= 500 then
            error_type = "Server Error (5xx)"
        else
            error_type = "Other Error"
        end
        
        local error_msg = string.format("[%s] Error #%d: %s - Status: %d - %s\n", 
            timestamp, error_count, error_type, status, tostring(body_text))
        
        -- 打印到控制台
        print(error_msg)
        
        -- 写入错误日志文件
        if error_log_file then
            error_log_file:write(error_msg)
            error_log_file:write("Headers: " .. tostring(headers) .. "\n")
            error_log_file:write("-" .. string.rep("-", 80) .. "\n")
            error_log_file:flush()
        end
        
        -- 记录特定错误类型
        if status >= 400 and status < 500 then
            print("Client error (4xx): " .. tostring(status))
        elseif status >= 500 then
            print("Server error (5xx): " .. tostring(status))
        end
    end
end

-- 设置阶段
function setup()
    error_count = 0  -- 重置错误计数器
    
    -- 初始化错误日志文件
    local timestamp = os.date("%Y%m%d_%H%M")
    local log_filename = "error_log_" .. timestamp .. ".txt"
    error_log_file = io.open(log_filename, "w")
    if error_log_file then
        error_log_file:write("=== Load Test Error Log ===\n")
        error_log_file:write("Started at: " .. os.date() .. "\n")
        error_log_file:write("Target URL: https://earndrop.prd.galaxy.eco/sahara/info\n")
        error_log_file:write("Loaded addresses: " .. tostring(#addresses) .. "\n")
        error_log_file:write("=" .. string.rep("=", 50) .. "\n\n")
        error_log_file:flush()
    end
    
    print("Setup: Loaded " .. tostring(#addresses) .. " addresses for load testing")
    print("Error log file: " .. (log_filename or "failed to create"))
end

function done(summary, latency, requests)
    print("Load test completed!")
    
    -- 关闭错误日志文件并写入总结
    if error_log_file then
        error_log_file:write("\n" .. string.rep("=", 50) .. "\n")
        error_log_file:write("LOAD TEST SUMMARY\n")
        error_log_file:write("Completed at: " .. os.date() .. "\n")
        error_log_file:write("Total errors logged: " .. tostring(error_count) .. "\n")
        error_log_file:close()
        print("Error log file closed")
    end
    
    -- 安全地访问summary字段
    if summary then
        print("Total requests: " .. tostring(summary.requests or 0))
        print("Total responses: " .. tostring(summary.responses or 0))
        
        -- 正确处理errors字段
        local wrk_error_count = 0
        if summary.errors then
            if type(summary.errors) == "table" then
                -- 如果errors是table，计算总数
                for _, count in pairs(summary.errors) do
                    if type(count) == "number" then
                        wrk_error_count = wrk_error_count + count
                    end
                end
            elseif type(summary.errors) == "number" then
                wrk_error_count = summary.errors
            end
        end
        print("Total errors: " .. tostring(wrk_error_count))
        print("Custom error count: " .. tostring(error_count))
        
        print("Total timeouts: " .. tostring(summary.timeouts or 0))
        
        -- 添加更多有用的统计信息
        if latency then
            print("Latency statistics:")
            print("  Min: " .. tostring(latency.min or 0) .. "ms")
            print("  Max: " .. tostring(latency.max or 0) .. "ms")
            print("  Mean: " .. tostring(latency.mean or 0) .. "ms")
            print("  Stdev: " .. tostring(latency.stdev or 0) .. "ms")
        end
        
        if requests then
            print("Request rate statistics:")
            print("  Min: " .. tostring(requests.min or 0) .. " req/s")
            print("  Max: " .. tostring(requests.max or 0) .. " req/s")
            print("  Mean: " .. tostring(requests.mean or 0) .. " req/s")
            print("  Stdev: " .. tostring(requests.stdev or 0) .. " req/s")
        end
    else
        print("Summary data not available")
    end
end 