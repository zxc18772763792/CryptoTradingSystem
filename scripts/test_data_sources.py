"""
数据源快速验证脚本

测试资金费率和恐惧贪婪指数采集器是否正常工作。
直接测试，避免依赖整个系统。
"""
import asyncio
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def test_funding_rate():
    """测试资金费率采集"""
    print("\n" + "=" * 60)
    print("Testing Funding Rate Collector")
    print("=" * 60)
    
    try:
        from core.data.funding_rate_collector import FundingRateCollector
        
        async with FundingRateCollector() as collector:
            # 测试单个交易所
            print("\n[Binance BTCUSDT]")
            rate = await collector.fetch_binance("BTCUSDT")
            if rate:
                print(f"  Funding Rate: {rate.funding_rate_pct:.4f}%")
                print(f"  Annualized:   {rate.annualized_rate:.2f}%")
                print(f"  Sentiment:    {rate.sentiment}")
                print(f"  Funding Time: {rate.funding_time}")
            else:
                print("  Failed to fetch")
            
            # 测试并行获取
            print("\n[All Exchanges - ETHUSDT]")
            rates = await collector.fetch_all("ETHUSDT")
            for exchange, r in rates.items():
                print(f"  {exchange.upper():10s}: {r.funding_rate_pct:.4f}%")
            
            # 测试预测费率
            print("\n[Predicted Rate - BTCUSDT]")
            predicted = await collector.fetch_binance_predicted("BTCUSDT")
            if predicted:
                print(f"  Mark Price:    ${predicted['mark_price']:,.2f}")
                print(f"  Index Price:   ${predicted['index_price']:,.2f}")
                print(f"  Est. Rate:     {predicted['last_funding_rate']*100:.4f}%")
                print(f"  Next Funding:  {predicted['next_funding_time']}")
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()


async def test_fear_greed():
    """测试恐惧贪婪指数"""
    print("\n" + "=" * 60)
    print("Testing Fear & Greed Index Collector")
    print("=" * 60)
    
    try:
        from core.data.sentiment.fear_greed_collector import FearGreedCollector
        
        async with FearGreedCollector() as collector:
            # 测试当前指数
            print("\n[Current Index]")
            index = await collector.fetch_current()
            if index:
                print(f"  Value:         {index.value}")
                print(f"  Classification: {index.classification}")
                print(f"  Signal:        {index.signal}")
                print(f"  Strength:      {index.signal_strength:.2f}")
            else:
                print("  Failed to fetch")
            
            # 测试历史
            print("\n[30-Day History]")
            history = await collector.fetch_history(30)
            if history:
                stats = collector.get_statistics(30)
                print(f"  Days fetched:  {len(history)}")
                print(f"  Mean:          {stats.get('mean', 0):.1f}")
                print(f"  Min:           {stats.get('min', 0)}")
                print(f"  Max:           {stats.get('max', 0)}")
                print(f"  Extreme Fear:  {stats.get('days_extreme_fear', 0)} days")
                print(f"  Extreme Greed: {stats.get('days_extreme_greed', 0)} days")
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()


async def test_factors():
    """测试因子计算"""
    print("\n" + "=" * 60)
    print("Testing Factor Computations")
    print("=" * 60)
    
    import pandas as pd
    import numpy as np
    
    # 测试资金费率因子
    print("\n[Funding Rate Factors]")
    from core.factors_ts.funding_rate_factors import (
        FundingRateFactor,
        FundingRateZscoreFactor,
        FundingRateExtremeFactor,
    )
    
    # 模拟数据
    np.random.seed(42)
    rates = np.random.normal(0.0001, 0.0002, 50)
    df = pd.DataFrame({"funding_rate": rates})
    
    factor1 = FundingRateFactor()
    result1 = factor1.compute(df)
    print(f"  FundingRateFactor: {result1.iloc[-1]:.6f}")
    
    factor2 = FundingRateZscoreFactor(period=30)
    result2 = factor2.compute(df)
    print(f"  ZscoreFactor:      {result2.iloc[-1]:.3f}")
    
    factor3 = FundingRateExtremeFactor(threshold=0.0005)
    result3 = factor3.compute(df)
    print(f"  ExtremeFactor:     {result3.iloc[-1]}")
    
    # 测试情绪因子
    print("\n[Sentiment Factors]")
    from core.factors_ts.sentiment_factors import (
        FearGreedFactor,
        ExtremeFearSignalFactor,
    )
    
    # 模拟数据
    values = [45, 50, 55, 40, 35, 25, 20, 22, 30, 45] + [50] * 20
    df = pd.DataFrame({"fear_greed_value": values})
    
    factor4 = FearGreedFactor()
    result4 = factor4.compute(df)
    print(f"  FearGreedFactor:   {result4.iloc[-1]}")
    
    factor5 = ExtremeFearSignalFactor(threshold=25)
    result5 = factor5.compute(df)
    print(f"  ExtremeFearSignal: {result5.iloc[-1]}")


async def main():
    """运行所有测试"""
    print("=" * 60)
    print("  Data Sources Quick Test")
    print("=" * 60)
    
    try:
        await test_funding_rate()
    except Exception as e:
        print(f"\nFunding Rate Test Error: {e}")
    
    try:
        await test_fear_greed()
    except Exception as e:
        print(f"\nFear & Greed Test Error: {e}")
    
    try:
        await test_factors()
    except Exception as e:
        print(f"\nFactors Test Error: {e}")
    
    print("\n" + "=" * 60)
    print("  Test Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())