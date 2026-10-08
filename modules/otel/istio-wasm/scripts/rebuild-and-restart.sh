#!/bin/bash

set -e  # 遇到错误立即退出

echo "🚀 开始重新编译WASM、安装插件和重启应用..."
echo "========================================"

# 1. 重新编译WASM
echo "📦 步骤1: 重新编译WASM..."
echo "执行: cargo build --target wasm32-unknown-unknown --release"
cargo build --target wasm32-unknown-unknown --release

if [ $? -eq 0 ]; then
    echo "✅ WASM编译成功"
else
    echo "❌ WASM编译失败"
    exit 1
fi

echo ""

# 2. 应用minimal.yaml重新安装插件
echo "🔧 步骤2: 重新安装WASM插件..."
echo "执行: kubectl apply -f deploy/minimal.yaml"
kubectl apply -f deploy/minimal.yaml

if [ $? -eq 0 ]; then
    echo "✅ WASM插件安装成功"
else
    echo "❌ WASM插件安装失败"
    exit 1
fi

echo ""

# 3. 重启demo-ota应用
echo "🔄 步骤3: 重启demo-ota应用..."
OTA_POD=$(kubectl get pods -l app=demo-ota -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$OTA_POD" ]; then
    echo "找到demo-ota pod: $OTA_POD"
    kubectl delete pod "$OTA_POD"
    echo "✅ demo-ota pod已删除，正在重启..."
else
    echo "⚠️  未找到demo-ota pod"
fi

echo ""

# 4. 重启demo-airline应用
echo "🔄 步骤4: 重启demo-airline应用..."
AIRLINE_POD=$(kubectl get pods -l app=demo-airline -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$AIRLINE_POD" ]; then
    echo "找到demo-airline pod: $AIRLINE_POD"
    kubectl delete pod "$AIRLINE_POD"
    echo "✅ demo-airline pod已删除，正在重启..."
else
    echo "⚠️  未找到demo-airline pod"
fi

echo ""

# 5. 等待pod重新启动
echo "⏳ 步骤5: 等待pod重新启动..."
echo "等待demo-ota pod就绪..."
kubectl wait --for=condition=ready pod -l app=demo-ota --timeout=60s

echo "等待demo-airline pod就绪..."
kubectl wait --for=condition=ready pod -l app=demo-airline --timeout=60s

echo ""

# 6. 显示最终状态
echo "📊 最终状态检查:"
echo "----------------------------------------"
echo "WASM插件状态:"
kubectl get wasmplugin -A

echo ""
echo "应用pod状态:"
kubectl get pods -l "app in (demo-ota,demo-airline)" -o wide

echo ""
echo "🎉 所有操作完成！"
echo "========================================"
echo "💡 提示:"
echo "- 可以使用 ./scripts/logs-demo-ota.sh 查看demo-ota日志"
echo "- 可以使用 ./scripts/logs-demo-airline.sh 查看demo-airline日志"
echo "- 可以通过 http://localhost:8080 访问应用"