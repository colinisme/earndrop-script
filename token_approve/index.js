const { ethers } = require("ethers");
require('dotenv').config();

// 检查环境变量
if (!process.env.PRIVATE_KEY) {
  console.error("错误: 请在 .env 文件中设置 PRIVATE_KEY");
  process.exit(1);
}

// Gravity链的RPC节点
const provider = new ethers.JsonRpcProvider("https://rpc.gravity.xyz");

// 从环境变量获取私钥
const privateKey = process.env.PRIVATE_KEY;
const wallet = new ethers.Wallet(privateKey, provider);

// 合约地址和ABI
const contractAddress = "0x121B747BbE636803adC85f437e149871f21Bc7B8";
const erc20Abi = [
  "function approve(address _spender, uint256 _value) public returns (bool)",
  "function balanceOf(address _owner) public view returns (uint256)",
  "function allowance(address _owner, address _spender) public view returns (uint256)"
];

// 实例化合约
const contract = new ethers.Contract(contractAddress, erc20Abi, wallet);

// 调用approve方法
async function approve(spender, amount) {
  try {
    console.log(`开始授权操作...`);
    console.log(`授权地址: ${spender}`);
    console.log(`授权金额: ${ethers.formatUnits(amount, 18)} 代币`);
    
    // 检查当前授权额度
    const currentAllowance = await contract.allowance(wallet.address, spender);
    console.log(`当前授权额度: ${ethers.formatUnits(currentAllowance, 18)} 代币`);
    
    // 检查钱包余额
    const balance = await contract.balanceOf(wallet.address);
    console.log(`钱包余额: ${ethers.formatUnits(balance, 18)} 代币`);
    
    // if (balance < amount) {
    //   throw new Error(`余额不足: 需要 ${ethers.formatUnits(amount, 18)} 代币，但只有 ${ethers.formatUnits(balance, 18)} 代币`);
    // }
    
    // 设置 gas 配置
    const gasEstimate = await contract.approve.estimateGas(spender, amount);
    const gasLimit = gasEstimate * 120n / 100n; // 增加20%的gas限制
    
    console.log(`预估 gas: ${gasEstimate.toString()}`);
    console.log(`设置 gas 限制: ${gasLimit.toString()}`);
    
    // 发送交易
    const tx = await contract.approve(spender, amount, {
      gasLimit: gasLimit
    });
    
    console.log("交易哈希:", tx.hash);
    console.log("等待交易确认...");
    
    const receipt = await tx.wait();
    console.log("交易确认成功!");
    console.log("区块号:", receipt.blockNumber);
    console.log("实际使用 gas:", receipt.gasUsed.toString());
    
    // 验证授权结果
    const newAllowance = await contract.allowance(wallet.address, spender);
    console.log(`新的授权额度: ${ethers.formatUnits(newAllowance, 18)} 代币`);
    
  } catch (error) {
    console.error("授权失败:", error.message);
    if (error.code) {
      console.error("错误代码:", error.code);
    }
    if (error.transaction) {
      console.error("交易详情:", error.transaction);
    }
    process.exit(1);
  }
}

// 检查命令行参数
const spenderAddress = process.argv[2];
const amount = process.argv[3];

if (!spenderAddress || !amount) {
  console.log("使用方法: node index.js <spender_address> <amount>");
  console.log("示例: node index.js 0x5d3e620828AD2Dd21E27d928BdbD3013fcE22013 100");
  process.exit(1);
}

// 验证地址格式
if (!ethers.isAddress(spenderAddress)) {
  console.error("错误: 无效的地址格式");
  process.exit(1);
}

// 验证金额格式
const amountInWei = ethers.parseUnits(amount, 18);
if (amountInWei <= 0n) {
  console.error("错误: 金额必须大于0");
  process.exit(1);
}

// 调用approve方法
approve(spenderAddress, amountInWei);