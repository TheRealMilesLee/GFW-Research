import pandas as pd
import glob
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats


# Combine all CSV files in the directory
all_files = glob.glob("dns_poisoning_results_*.csv")
df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

# Convert timestamp to datetime
df["timestamp"] = pd.to_datetime(df["timestamp"])
# Overall poisoning rate
poisoning_rate = df["is_poisoned"].mean()
print(f"Overall poisoning rate: {poisoning_rate:.2%}")

# Poisoning rate by domain
domain_poisoning = (
    df.groupby("domain")["is_poisoned"].mean().sort_values(ascending=False)
)
print("Top 10 most poisoned domains:")
print(domain_poisoning.head(10))

# Temporal analysis
df.set_index("timestamp", inplace=True)
daily_poisoning = df.resample("D")["is_poisoned"].mean()


# Poisoning rate over time
plt.figure(figsize=(12, 6))
daily_poisoning.plot()
plt.title("DNS Poisoning Rate Over Time")
plt.ylabel("Poisoning Rate")
plt.show()

# Heatmap of poisoning by domain and time
pivot = df.pivot_table(
    values="is_poisoned", index=df.index.date, columns="domain", aggfunc="mean"
)
plt.figure(figsize=(16, 10))
sns.heatmap(pivot, cmap="YlOrRd")
plt.title("DNS Poisoning Heatmap")
plt.show()


# Analyze types of poisoning
def categorize_poisoning(row):
    if not row["is_poisoned"]:
        return "Not Poisoned"
    if "NXDOMAIN" in str(row["china_result"]):
        return "NXDOMAIN"
    if isinstance(row["china_result"], list) and isinstance(row["global_result"], list):
        if set(row["china_result"]).issubset(set(row["global_result"])):
            return "Subset"
        else:
            return "Different IP"
    return "Other"


df["poisoning_type"] = df.apply(categorize_poisoning, axis=1)

poisoning_types = df["poisoning_type"].value_counts()
print("Types of DNS poisoning:")
print(poisoning_types)

# Analyze consistency of poisoning
poisoning_consistency = (
    df.groupby("domain")["is_poisoned"].std().sort_values(ascending=False)
)
print("\nDomains with most variable poisoning:")
print(poisoning_consistency.head())


# Example: T-test to compare poisoning rates between two time periods
period1 = df["2024-01-01":"2024-03-31"]["is_poisoned"]
period2 = df["2024-04-01":"2024-06-30"]["is_poisoned"]
t_stat, p_value = stats.ttest_ind(period1, period2)
print(f"T-test result: t-statistic = {t_stat}, p-value = {p_value}")


def generate_report(df, start_date, end_date):
    period_df = df[start_date:end_date]

    report = f"DNS Poisoning Report: {start_date} to {end_date}\n\n"
    report += f"Overall poisoning rate: {period_df['is_poisoned'].mean():.2%}\n"
    report += f"Most poisoned domain: {period_df.groupby('domain')['is_poisoned'].mean().idxmax()}\n"
    report += f"Least poisoned domain: {period_df.groupby('domain')['is_poisoned'].mean().idxmin()}\n"
    report += f"Total queries: {len(period_df)}\n"

    return report


print(generate_report(df, "2024-01-01", "2024-06-30"))
