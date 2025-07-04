# Streamlit 示例（量化交易挂单奖励监控）
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

st.title("Market-Making Incentive Dashboard")

# 模拟数据
st.subheader("Aggregate Incentive Distribution")
depth = np.linspace(0.001, 0.02, 100)
reward = 1 / (depth**2)
df = pd.DataFrame({"Depth (log-price)": depth, "Incentive Weight": reward})
fig = px.area(df, x="Depth (log-price)", y="Incentive Weight", title="Reward vs. Depth")
st.plotly_chart(fig)

# 假设 trader 策略分布
st.subheader("Trader Strategy Snapshot")
trader_types = ["Conservative", "Balanced", "Aggressive"]
ask_bid_diff = [0.008, 0.005, 0.002]
inventory_risk = [0.2, 0.5, 0.8]
df2 = pd.DataFrame({"Trader Type": trader_types, "Spread Width": ask_bid_diff, "Risk Appetite": inventory_risk})
st.dataframe(df2)
