#!/usr/bin/env python3
"""
Plot querier benchmark results.
Generates PDF plots with Type 42 fonts.
"""

import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np

# Configure matplotlib for PDF with Type 42 fonts
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
matplotlib.rcParams['font.family'] = 'sans-serif'
matplotlib.rcParams['font.size'] = 28
matplotlib.rcParams['axes.labelsize'] = 72
matplotlib.rcParams['axes.titlesize'] = 72
matplotlib.rcParams['xtick.labelsize'] = 65
matplotlib.rcParams['ytick.labelsize'] = 65
matplotlib.rcParams['legend.fontsize'] = 72
matplotlib.rcParams['legend.title_fontsize'] = 72


def parse_time(time_str):
    """Parse time string like '47.2s' or '38ms' to seconds."""
    if pd.isna(time_str):
        return np.nan
    time_str = str(time_str).strip()
    if time_str.endswith('ms'):
        return float(time_str[:-2]) / 1000
    elif time_str.endswith('s'):
        return float(time_str[:-1])
    else:
        return float(time_str)


def parse_size(size_str):
    """Parse size string like '218.8 KB' or '408 B' to bytes."""
    if pd.isna(size_str):
        return np.nan
    size_str = str(size_str).strip()
    match = re.match(r'([\d.]+)\s*(B|KB|MB|GB)', size_str)
    if match:
        value = float(match.group(1))
        unit = match.group(2)
        if unit == 'B':
            return value
        elif unit == 'KB':
            return value * 1024
        elif unit == 'MB':
            return value * 1024 * 1024
        elif unit == 'GB':
            return value * 1024 * 1024 * 1024
    return float(size_str)


def load_querier_data(directory):
    """Load all querier benchmark CSV files."""
    files = [
        'samples_sweep_results.csv',
        'cm_sweep_results.csv',
        'histogram_sweep_results.csv'
    ]

    all_data = []
    for filename in files:
        filepath = os.path.join(directory, filename)
        if not os.path.exists(filepath):
            print(f"Warning: {filepath} not found")
            continue

        try:
            df = pd.read_csv(filepath)
            if df.empty:
                continue

            # Extract mode from filename
            mode = filename.replace('_sweep_results.csv', '')
            df['mode'] = mode

            # Parse time and size columns
            df['prove_time_sec'] = df['prove_time_ms'].apply(parse_time)
            df['verify_time_sec'] = df['verify_time_ms'].apply(parse_time)
            df['proof_size_bytes_parsed'] = df['proof_size_bytes'].apply(parse_size)
            df['journal_size_bytes_parsed'] = df['journal_size_bytes'].apply(parse_size)

            all_data.append(df)
        except Exception as e:
            print(f"Warning: Could not read {filepath}: {e}")

    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


def plot_metric_by_mode(df, output_path, metric_col, ylabel, title, transform=None, legend_loc=None):
    """Plot metric with 3 subplots (one per mode), query types within each subplot."""
    modes = ['samples', 'cm', 'histogram']
    mode_titles = {'samples': 'Hash Table', 'cm': 'Count-Min Sketch', 'histogram': 'Histogram'}

    fig, axes = plt.subplots(1, 3, figsize=(48, 12),
                             gridspec_kw={'width_ratios': [1, 1, 1]})

    colors = plt.cm.tab10(np.linspace(0, 1, 10))
    hatches = ['', '/', '\\', 'x', '.', 'o', '+', '*']

    for idx, mode in enumerate(modes):
        ax = axes[idx]
        mode_data = df[(df['mode'] == mode) & (df['dp_enabled'] == 0)]
        # Exclude max_key from raw logs (samples) plots
        if mode == 'samples':
            mode_data = mode_data[mode_data['query_type'] != 'raw/max_key']

        if mode_data.empty:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center', fontsize=18)
            continue

        query_types = sorted(mode_data['query_type'].unique())
        epochs_list = sorted(mode_data['epochs'].unique())
        n_bars = len(query_types)

        # Calculate bar positions
        x_positions = np.arange(len(epochs_list))
        bar_width = 0.8 / n_bars

        for i, qtype in enumerate(query_types):
            data = mode_data[mode_data['query_type'] == qtype].copy()
            grouped = data.groupby('epochs')[metric_col].mean().reset_index()
            grouped = grouped.sort_values('epochs')

            # Get y values for each epoch
            y_values = []
            for epoch in epochs_list:
                epoch_data = grouped[grouped['epochs'] == epoch]
                if not epoch_data.empty:
                    val = epoch_data[metric_col].values[0]
                    if transform:
                        val = transform(pd.Series([val])).values[0]
                    y_values.append(val)
                else:
                    y_values.append(0)

            # Use short label (remove mode prefix)
            short_label = qtype.split('/')[-1] if '/' in qtype else qtype
            if short_label == 'sum_key':
                short_label = 'sum_by_key'
            elif short_label == 'topk':
                short_label = 'Top-10'
            elif short_label == 'sum_topk':
                short_label = 'sum_Top-10'
            elif short_label == 'estimate':
                short_label = 'Estimate One Key'
            elif short_label == 'p90':
                short_label = 'P90'

            bar_positions = x_positions + (i - (n_bars - 1) / 2) * bar_width
            hatch = hatches[i % len(hatches)]
            ax.bar(bar_positions, y_values, bar_width, label=short_label,
                   color=colors[i], hatch=hatch, edgecolor='black', linewidth=0.5)

        ax.set_title(mode_titles[mode])
        ax.set_xlabel('Queried Epochs')
        if idx == 0:
            ax.set_ylabel(ylabel)
        if legend_loc:
            ax.legend(loc=legend_loc)
        else:
            ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        ax.set_xticks(x_positions)
        ax.set_xticklabels([str(e) for e in epochs_list], rotation=45, ha='right')
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    plt.savefig(output_path, format='pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")




def plot_proof_and_journal_size(df, output_path):
    """Plot proof + journal size with stacked bars."""
    modes = ['samples', 'cm', 'histogram']
    mode_titles = {'samples': 'Hash Table', 'cm': 'Count-Min Sketch', 'histogram': 'Histogram'}

    fig, axes = plt.subplots(1, 3, figsize=(48, 12),
                             gridspec_kw={'width_ratios': [1, 1, 1]})

    colors = plt.cm.tab10(np.linspace(0, 1, 10))
    hatches = ['', '/', '\\', 'x', '.', 'o', '+', '*']

    for idx, mode in enumerate(modes):
        ax = axes[idx]
        mode_data = df[(df['mode'] == mode) & (df['dp_enabled'] == 0)]
        # Exclude max_key from raw logs (samples) plots
        if mode == 'samples':
            mode_data = mode_data[mode_data['query_type'] != 'raw/max_key']

        if mode_data.empty:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center', fontsize=18)
            continue

        query_types = sorted(mode_data['query_type'].unique())
        epochs_list = sorted(mode_data['epochs'].unique())
        n_bars = len(query_types)

        # Calculate bar positions
        x_positions = np.arange(len(epochs_list))
        bar_width = 0.8 / n_bars

        for i, qtype in enumerate(query_types):
            data = mode_data[mode_data['query_type'] == qtype].copy()
            grouped = data.groupby('epochs').agg({
                'proof_size_bytes_parsed': 'mean',
                'journal_size_bytes_parsed': 'mean'
            }).reset_index()
            grouped = grouped.sort_values('epochs')

            # Get y values for each epoch
            proof_values = []
            journal_values = []
            for epoch in epochs_list:
                epoch_data = grouped[grouped['epochs'] == epoch]
                if not epoch_data.empty:
                    proof_values.append(epoch_data['proof_size_bytes_parsed'].values[0] / 1024)
                    journal_values.append(epoch_data['journal_size_bytes_parsed'].values[0] / 1024)
                else:
                    proof_values.append(0)
                    journal_values.append(0)

            # Use short label (remove mode prefix)
            short_label = qtype.split('/')[-1] if '/' in qtype else qtype
            if short_label == 'sum_key':
                short_label = 'sum_by_key'
            elif short_label == 'topk':
                short_label = 'Top-10'
            elif short_label == 'sum_topk':
                short_label = 'sum_Top-10'
            elif short_label == 'estimate':
                short_label = 'Estimate One Key'
            elif short_label == 'p90':
                short_label = 'P90'

            bar_positions = x_positions + (i - (n_bars - 1) / 2) * bar_width
            hatch = hatches[i % len(hatches)]

            # Plot proof size
            ax.bar(bar_positions, proof_values, bar_width,
                   label=f'{short_label} Proof', color=colors[i],
                   hatch=hatch, edgecolor='black', linewidth=0.5)
            # Plot journal size stacked on top
            ax.bar(bar_positions, journal_values, bar_width, bottom=proof_values,
                   label=f'{short_label} Journal', color=colors[i] * 0.3,
                   hatch=hatch, edgecolor='black', linewidth=0.5)

        ax.set_title(mode_titles[mode])
        ax.set_xlabel('Queried Epochs')
        if idx == 0:
            ax.set_ylabel('Size (KB)')
        ax.legend(ncol=2)
        ax.grid(True, alpha=0.3, axis='y')
        ax.set_xticks(x_positions)
        ax.set_xticklabels([str(e) for e in epochs_list], rotation=45, ha='right')
        ax.set_ylim(bottom=0)

    plt.tight_layout()
    plt.savefig(output_path, format='pdf', bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path}")


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Create figures output directory
    figures_dir = os.path.join(script_dir, 'figures')
    os.makedirs(figures_dir, exist_ok=True)

    print("Loading querier benchmark data...")
    df = load_querier_data(script_dir)

    if df.empty:
        print("No data found!")
        return

    print(f"Loaded {len(df)} records")
    print(f"Query types: {sorted(df['query_type'].unique())}")
    print(f"Modes: {df['mode'].unique()}")
    print(f"Epochs: {sorted(df['epochs'].unique())}")

    print(f"\nGenerating plots to {figures_dir}...")

    # Proof time plot
    plot_metric_by_mode(
        df,
        os.path.join(figures_dir, 'querier_proof_time.pdf'),
        'prove_time_sec',
        'Proof Generation\nTime (Min)',
        'Querier Proof Time vs Epochs',
        transform=lambda y: y / 60
    )

    # Verify time plot
    plot_metric_by_mode(
        df,
        os.path.join(figures_dir, 'querier_verify_time.pdf'),
        'verify_time_sec',
        'Proof Verification\nTime (ms)',
        'Querier Verification Time vs Epochs',
        transform=lambda y: y * 1000,
        legend_loc='lower right'
    )

    # Proof size plot
    plot_metric_by_mode(
        df,
        os.path.join(figures_dir, 'querier_proof_size.pdf'),
        'proof_size_bytes_parsed',
        'Proof Size (KB)',
        'Querier Proof Size vs Epochs',
        transform=lambda y: y / 1024,
        legend_loc='lower right'
    )

    # Journal size plot
    plot_metric_by_mode(
        df,
        os.path.join(figures_dir, 'querier_journal_size.pdf'),
        'journal_size_bytes_parsed',
        'Public Output\nSize (KB)',
        'Querier Journal Size vs Epochs',
        transform=lambda y: y / 1024
    )

    # Memory usage plot
    plot_metric_by_mode(
        df,
        os.path.join(figures_dir, 'querier_memory.pdf'),
        'memory_mb',
        'Memory (GB)',
        'Querier Memory Usage vs Epochs',
        transform=lambda y: y / 1024
    )

    print("\nDone! Generated 5 PDF plots.")


if __name__ == '__main__':
    main()
