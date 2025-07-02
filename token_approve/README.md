# Token Approve Script

这是一个用于在 Gravity 链上进行代币授权的安全脚本。

## ⚠️ 安全警告

- **永远不要在代码中硬编码私钥**
- **确保 .env 文件不会被提交到版本控制系统**
- **定期检查钱包余额和授权额度**

## 安装

1. 安装依赖：
```bash
npm install
```

2. 配置环境变量：
```bash
cp env.example .env
```

3. 编辑 `.env` 文件，填入你的私钥：
```
PRIVATE_KEY=your_private_key_here_without_0x_prefix
```

## 使用方法

```bash
node index.js <spender_address> <amount>
```

### 示例

```bash
# 授权 100 个代币给指定地址
node index.js 0x5d3e620828AD2Dd21E27d928BdbD3013fcE22013 100
```

## 功能特性

- ✅ 安全的私钥管理（环境变量）
- ✅ 余额检查
- ✅ 当前授权额度检查
- ✅ Gas 费用估算和配置
- ✅ 详细的错误处理
- ✅ 交易确认和验证
- ✅ 地址格式验证
- ✅ 金额验证

## 注意事项

1. 确保钱包中有足够的代币余额
2. 确保钱包中有足够的 ETH 支付 gas 费用
3. 仔细检查授权地址是否正确
4. 建议先用小额测试

## 合约信息

- 代币合约地址: `0x121B747BbE636803adC85f437e149871f21Bc7B8`
- 网络: Gravity Chain
- RPC: `https://rpc.gravity.xyz` 