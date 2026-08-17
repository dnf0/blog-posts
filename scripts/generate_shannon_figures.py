#!/usr/bin/env python3
"""
Generate publication-quality, mathematically rigorous diagrams for the Shannon Entropy blog post.
Includes all relevant variables: M, N, x_i, n_i, p_i, b_i, H(p).
"""

import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.gridspec import GridSpec

# Output directories
OUTPUT_DIRS = [
    "/Users/danielfisher/repos/dnf0.github.io/public/images/shannon",
    "/Users/danielfisher/repos/blog-posts/content/images/shannon",
    "/Users/danielfisher/repos/blog-posts/shannon-entropy-lossless-compression/images"
]

for d in OUTPUT_DIRS:
    os.makedirs(d, exist_ok=True)

# Shared styling
plt.rcParams['font.sans-serif'] = 'DejaVu Sans', 'Arial', 'Helvetica'
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['mathtext.fontset'] = 'cm'

BG_COLOR = '#0f172a'      # Slate 900
PANEL_BG = '#1e293b'      # Slate 800
TEXT_MAIN = '#f8fafc'     # Slate 50
TEXT_MUTED = '#94a3b8'    # Slate 400
EMERALD = '#10b981'
CYAN = '#06b6d4'
AMBER = '#f59e0b'
ROSE = '#f43f5e'
PURPLE = '#a855f7'
BORDER_COLOR = '#334155'  # Slate 700


def save_fig(fig, filename):
    for d in OUTPUT_DIRS:
        filepath = os.path.join(d, filename)
        fig.savefig(filepath, dpi=300, bbox_inches='tight', facecolor=BG_COLOR, edgecolor='none')
    print(f"Saved: {filename}")
    plt.close(fig)


# -------------------------------------------------------------
# FIGURE 1: The Setup (Alphabet M, Sequence N, Counts n_i, Probs p_i)
# -------------------------------------------------------------
def create_fig1():
    fig = plt.figure(figsize=(12, 6.5), facecolor=BG_COLOR)
    gs = GridSpec(2, 2, height_ratios=[1, 1.2], width_ratios=[1.2, 1], hspace=0.35, wspace=0.25)

    # Top: Alphabet M and Sequence N
    ax1 = fig.add_subplot(gs[0, :], facecolor=PANEL_BG)
    ax1.axis('off')

    # Alphabet Header
    ax1.text(0.02, 0.82, r"Alphabet: $M = 4$ Distinct Symbols ($\mathcal{X} = \{x_1, x_2, x_3, x_4\}$)",
             color=TEXT_MAIN, fontsize=13, fontweight='bold')
    
    symbols = [('x₁: Sunny', AMBER), ('x₂: Cloudy', CYAN), ('x₃: Rainy', EMERALD), ('x₄: Snowy', PURPLE)]
    for idx, (sym, col) in enumerate(symbols):
        rect = patches.FancyBboxPatch((0.02 + idx * 0.24, 0.48), 0.22, 0.24,
                                      boxstyle="round,pad=0.03", fc=BG_COLOR, ec=col, lw=1.5)
        ax1.add_patch(rect)
        ax1.text(0.13 + idx * 0.24, 0.60, sym, color=TEXT_MAIN, fontsize=11, ha='center', va='center', fontweight='semibold')

    # Sequence Header
    ax1.text(0.02, 0.28, r"Data Sequence: Length $N = 20$ Transmitted Symbols", color=TEXT_MAIN, fontsize=12, fontweight='bold')
    
    seq_symbols = [
        'x₁', 'x₁', 'x₂', 'x₁', 'x₃', 'x₁', 'x₂', 'x₁', 'x₄', 'x₁',
        'x₂', 'x₁', 'x₁', 'x₃', 'x₁', 'x₂', 'x₁', 'x₃', 'x₂', 'x₂'
    ]
    sym_colors = {'x₁': AMBER, 'x₂': CYAN, 'x₃': EMERALD, 'x₄': PURPLE}
    for idx, s in enumerate(seq_symbols):
        rect = patches.Rectangle((0.02 + idx * 0.048, 0.04), 0.042, 0.18, fc=BG_COLOR, ec=sym_colors[s], lw=1)
        ax1.add_patch(rect)
        ax1.text(0.041 + idx * 0.048, 0.13, s, color=sym_colors[s], fontsize=9, ha='center', va='center', fontweight='bold')

    # Bottom Left: Frequency Counts n_i
    ax2 = fig.add_subplot(gs[1, 0], facecolor=PANEL_BG)
    labels = [r'$x_1$ (Sunny)', r'$x_2$ (Cloudy)', r'$x_3$ (Rainy)', r'$x_4$ (Snowy)']
    counts = [10, 6, 3, 1]
    cols = [AMBER, CYAN, EMERALD, PURPLE]
    
    bars = ax2.bar(labels, counts, color=cols, edgecolor=BORDER_COLOR, width=0.55, lw=1.2)
    ax2.set_ylabel('Frequency Count ($n_i$)', color=TEXT_MAIN, fontsize=11)
    ax2.set_title(r'1. Count Frequencies: $\sum_{i=1}^M n_i = N = 20$', color=TEXT_MAIN, fontsize=12, pad=10, fontweight='bold')
    ax2.tick_params(colors=TEXT_MUTED, labelsize=10)
    ax2.set_ylim(0, 12)
    ax2.grid(axis='y', linestyle='--', alpha=0.2, color=TEXT_MUTED)
    for spine in ax2.spines.values():
        spine.set_edgecolor(BORDER_COLOR)

    for bar, c in zip(bars, counts):
        yval = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, yval + 0.3, f'$n_i = {c}$', ha='center', va='bottom', color=TEXT_MAIN, fontweight='bold', fontsize=10)

    # Bottom Right: Normalized Probabilities p_i
    ax3 = fig.add_subplot(gs[1, 1], facecolor=PANEL_BG)
    probs = [c / 20.0 for c in counts]
    
    bars3 = ax3.bar(labels, probs, color=cols, edgecolor=BORDER_COLOR, width=0.55, lw=1.2)
    ax3.set_ylabel('Probability ($p_i = n_i / N$)', color=TEXT_MAIN, fontsize=11)
    ax3.set_title(r'2. Normalise to Probabilities: $\sum_{i=1}^M p_i = 1.0$', color=TEXT_MAIN, fontsize=12, pad=10, fontweight='bold')
    ax3.tick_params(colors=TEXT_MUTED, labelsize=10)
    ax3.set_ylim(0, 0.65)
    ax3.grid(axis='y', linestyle='--', alpha=0.2, color=TEXT_MUTED)
    for spine in ax3.spines.values():
        spine.set_edgecolor(BORDER_COLOR)

    for bar, p in zip(bars3, probs):
        yval = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2, yval + 0.015, f'{p:.2f}', ha='center', va='bottom', color=TEXT_MAIN, fontweight='bold', fontsize=10)

    plt.suptitle(r"The Fundamental Setup: Alphabet Size $M$, Sequence Length $N$, Frequencies $n_i$, and Probabilities $p_i$",
                 color=TEXT_MAIN, fontsize=14, fontweight='bold', y=0.98)
    save_fig(fig, "01_data_stream.png")


# -------------------------------------------------------------
# FIGURE 2: Information / Surprise Curve I(p) = -log2(p)
# -------------------------------------------------------------
def create_fig2():
    fig, ax = plt.subplots(figsize=(11, 6), facecolor=BG_COLOR)
    ax.set_facecolor(PANEL_BG)

    p = np.linspace(0.01, 1.0, 500)
    I = -np.log2(p)

    ax.plot(p, I, color=EMERALD, lw=3.5, label=r'$I(p) = \log_2(1/p) = -\log_2(p)$ (Bits of Surprise)')

    # Key Milestone Points
    milestones = [
        (1.0, 0.0, r'Certain Outcome ($p = 1.0$)' '\n' r'$0$ bits (Zero surprise)', TEXT_MUTED, 'bottom', 0.25),
        (0.5, 1.0, r'Fair Coin Flip ($p = 0.5$)' '\n' r'$1.0$ bit of surprise', CYAN, 'bottom', 0.45),
        (0.25, 2.0, r'4-Sided Outcome ($p = 0.25$)' '\n' r'$2.0$ bits of surprise', AMBER, 'bottom', 0.5),
        (0.125, 3.0, r'8-Sided Outcome ($p = 0.125$)' '\n' r'$3.0$ bits of surprise', PURPLE, 'bottom', 0.5),
        (0.03125, 5.0, r'Rare Anomaly ($p \approx 0.03$)' '\n' r'$5.0$ bits of surprise', ROSE, 'left', 0.2)
    ]

    for p_val, i_val, txt, col, align, offset in milestones:
        ax.plot(p_val, i_val, 'o', color=col, markersize=9, markeredgecolor=TEXT_MAIN, markeredgewidth=1.5)
        if align == 'left':
            ax.annotate(txt, xy=(p_val, i_val), xytext=(p_val + 0.03, i_val + 0.1),
                        color=TEXT_MAIN, fontsize=10, fontweight='semibold',
                        arrowprops=dict(arrowstyle='->', color=col, lw=1.5))
        else:
            ax.annotate(txt, xy=(p_val, i_val), xytext=(p_val - 0.08, i_val + offset),
                        color=TEXT_MAIN, fontsize=10, fontweight='semibold',
                        arrowprops=dict(arrowstyle='->', color=col, lw=1.5))

    # Add Takeaway Box
    bbox_props = dict(boxstyle="round,pad=0.5", fc=BG_COLOR, ec=BORDER_COLOR, lw=1.5)
    ax.text(0.52, 4.2, 
            r"$\mathbf{Why\ base\ 2\ logarithms?}$" "\n"
            r"• Probabilities multiply: $P(A \cap B) = P(A) \times P(B)$" "\n"
            r"• Information adds: $I(A \cap B) = I(A) + I(B)$" "\n"
            r"• $\log_2(1/(p_A p_B)) = -\log_2(p_A) - \log_2(p_B)$" "\n"
            r"• Base 2 gives binary Yes/No decision questions (bits).",
            color=TEXT_MAIN, fontsize=10.5, linespacing=1.4, bbox=bbox_props)

    ax.set_xlabel('Probability of Event ($p_i$)', color=TEXT_MAIN, fontsize=12, labelpad=10)
    ax.set_ylabel(r'Information Content / Surprise ($b_i = -\log_2 p_i$ bits)', color=TEXT_MAIN, fontsize=12, labelpad=10)
    ax.set_title(r"Quantifying Surprise: Why Information Scales Inversely as $I(x_i) = -\log_2(p_i)$",
                 color=TEXT_MAIN, fontsize=14, pad=15, fontweight='bold')
    ax.set_xlim(-0.02, 1.05)
    ax.set_ylim(-0.2, 6.8)
    ax.tick_params(colors=TEXT_MUTED, labelsize=10)
    ax.grid(True, linestyle='--', alpha=0.2, color=TEXT_MUTED)
    for spine in ax.spines.values():
        spine.set_edgecolor(BORDER_COLOR)

    save_fig(fig, "02_surprise_meter.png")


# -------------------------------------------------------------
# FIGURE 3: Entropy Extremes (H = 0 vs Uniform H = log2 M)
# -------------------------------------------------------------
def create_fig3():
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(14, 5.5), facecolor=BG_COLOR)
    symbols = ['$x_1$', '$x_2$', '$x_3$', '$x_4$']

    # 1. Extreme 1: Certainty
    ax1.set_facecolor(PANEL_BG)
    p1 = [1.0, 0.0, 0.0, 0.0]
    ax1.bar(symbols, p1, color=[EMERALD, BORDER_COLOR, BORDER_COLOR, BORDER_COLOR], width=0.55, edgecolor=BORDER_COLOR)
    ax1.set_ylim(0, 1.15)
    ax1.set_title(r"Extreme 1: Pure Certainty" "\n" r"$p_1 = 1.0, p_{i \neq 1} = 0.0$", color=TEXT_MAIN, fontsize=12, fontweight='bold')
    ax1.set_ylabel("Probability ($p_i$)", color=TEXT_MAIN, fontsize=10)
    ax1.tick_params(colors=TEXT_MUTED, labelsize=10)
    ax1.grid(axis='y', linestyle='--', alpha=0.2, color=TEXT_MUTED)
    for spine in ax1.spines.values():
        spine.set_edgecolor(BORDER_COLOR)
    
    # Entropy result box
    ax1.text(0.5, 0.65, r"$\mathbf{H(p) = 0.0\text{ bits}}$" "\n"
             r"No uncertainty." "\n"
             r"0 bits needed to transmit.",
             color=EMERALD, fontsize=11, ha='center', va='center',
             transform=ax1.transAxes, bbox=dict(boxstyle="round,pad=0.4", fc=BG_COLOR, ec=EMERALD, lw=1.5))

    # 2. Arbitrary Biased
    ax2.set_facecolor(PANEL_BG)
    p2 = [0.60, 0.25, 0.10, 0.05]
    cols2 = [AMBER, CYAN, EMERALD, PURPLE]
    ax2.bar(symbols, p2, color=cols2, width=0.55, edgecolor=BORDER_COLOR)
    ax2.set_ylim(0, 1.15)
    ax2.set_title(r"Realistic: Skewed Distribution" "\n" r"$p = [0.60, 0.25, 0.10, 0.05]$", color=TEXT_MAIN, fontsize=12, fontweight='bold')
    ax2.tick_params(colors=TEXT_MUTED, labelsize=10)
    ax2.grid(axis='y', linestyle='--', alpha=0.2, color=TEXT_MUTED)
    for spine in ax2.spines.values():
        spine.set_edgecolor(BORDER_COLOR)
    
    H_val = -sum(p * np.log2(p) for p in p2)
    ax2.text(0.5, 0.65, f"$\\mathbf{{H(p) = {H_val:.2f}\\text{{ bits}}}}$\n"
             r"Moderate uncertainty." "\n"
             r"Compressible via variable codes.",
             color=CYAN, fontsize=11, ha='center', va='center',
             transform=ax2.transAxes, bbox=dict(boxstyle="round,pad=0.4", fc=BG_COLOR, ec=CYAN, lw=1.5))

    # 3. Extreme 2: Uniform / Max Entropy
    ax3.set_facecolor(PANEL_BG)
    p3 = [0.25, 0.25, 0.25, 0.25]
    ax3.bar(symbols, p3, color=PURPLE, width=0.55, edgecolor=BORDER_COLOR)
    ax3.set_ylim(0, 1.15)
    ax3.set_title(r"Extreme 2: Maximum Chaos" "\n" r"Uniform: $p_i = 1/M = 0.25$", color=TEXT_MAIN, fontsize=12, fontweight='bold')
    ax3.tick_params(colors=TEXT_MUTED, labelsize=10)
    ax3.grid(axis='y', linestyle='--', alpha=0.2, color=TEXT_MUTED)
    for spine in ax3.spines.values():
        spine.set_edgecolor(BORDER_COLOR)
    
    ax3.text(0.5, 0.65, r"$\mathbf{H(p) = \log_2(M) = 2.0\text{ bits}}$" "\n"
             r"Maximum uncertainty." "\n"
             r"Incompressible (zero redundancy).",
             color=PURPLE, fontsize=11, ha='center', va='center',
             transform=ax3.transAxes, bbox=dict(boxstyle="round,pad=0.4", fc=BG_COLOR, ec=PURPLE, lw=1.5))

    plt.suptitle(r"The Bounds of Shannon Entropy: $0 \leq H(p) \leq \log_2(M)$",
                 color=TEXT_MAIN, fontsize=14, fontweight='bold', y=0.98)
    plt.tight_layout()
    save_fig(fig, "03_two_extremes.png")


# -------------------------------------------------------------
# FIGURE 4: Fixed vs Optimal Variable Coding Comparison
# -------------------------------------------------------------
def create_fig4():
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 6.5), facecolor=BG_COLOR)
    fig.subplots_adjust(hspace=0.4)

    # Top: Naive Fixed-Length
    ax1.set_facecolor(PANEL_BG)
    ax1.axis('off')
    ax1.set_xlim(0, 1)
    ax1.set_ylim(0, 1)

    ax1.text(0.02, 0.85, r"A. Naive Fixed-Length Encoding ($b = \lceil \log_2 M \rceil = 2$ bits per symbol)",
             color=TEXT_MAIN, fontsize=12, fontweight='bold')
    
    fixed_table = [
        (r'$x_1$ ($p_1 = 0.60$)', '00', '2 bits'),
        (r'$x_2$ ($p_2 = 0.25$)', '01', '2 bits'),
        (r'$x_3$ ($p_3 = 0.10$)', '10', '2 bits'),
        (r'$x_4$ ($p_4 = 0.05$)', '11', '2 bits')
    ]
    for idx, (sym, code, length) in enumerate(fixed_table):
        rect = patches.Rectangle((0.02 + idx * 0.24, 0.28), 0.22, 0.42, fc=BG_COLOR, ec=BORDER_COLOR, lw=1.2)
        ax1.add_patch(rect)
        ax1.text(0.13 + idx * 0.24, 0.55, sym, color=TEXT_MAIN, fontsize=10.5, ha='center')
        ax1.text(0.13 + idx * 0.24, 0.38, f"Code: {code} ({length})", color=CYAN, fontsize=10, ha='center', fontweight='bold')

    ax1.text(0.02, 0.08, r"Expected Code Length: $\mathbb{E}[b_{\text{fixed}}] = \sum p_i \cdot 2 = \mathbf{2.00\text{ bits/symbol}}$ (Wastes bandwidth on frequent $x_1$)",
             color=ROSE, fontsize=10.5, fontweight='semibold')

    # Bottom: Optimal Variable-Length
    ax2.set_facecolor(PANEL_BG)
    ax2.axis('off')
    ax2.set_xlim(0, 1)
    ax2.set_ylim(0, 1)

    ax2.text(0.02, 0.85, r"B. Shannon Optimal Variable-Length Encoding ($b_i \approx -\log_2 p_i$)",
             color=TEXT_MAIN, fontsize=12, fontweight='bold')

    var_table = [
        (r'$x_1$ ($p_1 = 0.60$)', '0', '1 bit', AMBER),
        (r'$x_2$ ($p_2 = 0.25$)', '10', '2 bits', CYAN),
        (r'$x_3$ ($p_3 = 0.10$)', '110', '3 bits', EMERALD),
        (r'$x_4$ ($p_4 = 0.05$)', '111', '3 bits', PURPLE)
    ]
    for idx, (sym, code, length, col) in enumerate(var_table):
        rect = patches.Rectangle((0.02 + idx * 0.24, 0.28), 0.22, 0.42, fc=BG_COLOR, ec=col, lw=1.5)
        ax2.add_patch(rect)
        ax2.text(0.13 + idx * 0.24, 0.55, sym, color=TEXT_MAIN, fontsize=10.5, ha='center')
        ax2.text(0.13 + idx * 0.24, 0.38, f"Code: {code} ({length})", color=col, fontsize=10, ha='center', fontweight='bold')

    ax2.text(0.02, 0.08, r"Expected Code Length: $\mathbb{E}[b] = 0.60(1) + 0.25(2) + 0.10(3) + 0.05(3) = \mathbf{1.55\text{ bits/symbol}}$ ($\mathbf{22.5\%}$ space savings $\approx H(p)$)",
             color=EMERALD, fontsize=10.5, fontweight='semibold')

    plt.suptitle(r"Shannon's Source Coding Principle: Frequent Symbols Get Shorter Bits",
                 color=TEXT_MAIN, fontsize=14, fontweight='bold', y=0.98)
    save_fig(fig, "04_coding_comparison.png")


# -------------------------------------------------------------
# FIGURE 5: Deep Neural Compression Pipeline
# -------------------------------------------------------------
def create_fig5():
    fig, ax = plt.subplots(figsize=(13, 5.5), facecolor=BG_COLOR)
    ax.set_facecolor(PANEL_BG)
    ax.axis('off')
    ax.set_xlim(0, 1.2)
    ax.set_ylim(0, 1)

    # Modules
    stages = [
        (0.05, 0.35, 0.16, 0.35, r"Input Tensor $x$" "\n" r"$x \in \mathbb{R}^{C \times H \times W}$" "\n" r"(Geospatial)", CYAN),
        (0.26, 0.35, 0.16, 0.35, r"Neural Encoder" "\n" r"$g_a(x; \phi)$" "\n" r"(Non-linear CNN/ViT)", EMERALD),
        (0.47, 0.35, 0.16, 0.35, r"Latent Bottleneck" "\n" r"$y \to \hat{y} = Q(y)$" "\n" r"(Quantised Latents)", AMBER),
        (0.68, 0.35, 0.16, 0.35, r"Entropy Coder" "\n" r"$\mathbb{E}[-\log_2 p_{\hat{y}}(\hat{y})]$" "\n" r"(Learned Hyperprior)", PURPLE),
        (0.89, 0.35, 0.16, 0.35, r"Neural Decoder" "\n" r"$g_s(\hat{y}; \theta) \to \hat{x}$" "\n" r"(Reconstruction)", CYAN)
    ]

    for (x, y, w, h, text, col) in stages:
        box = patches.FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.03", fc=BG_COLOR, ec=col, lw=2)
        ax.add_patch(box)
        ax.text(x + w/2, y + h/2, text, color=TEXT_MAIN, fontsize=10.5, ha='center', va='center', fontweight='semibold', linespacing=1.3)

    # Arrows
    arrow_props = dict(facecolor=TEXT_MUTED, edgecolor='none', width=2.5, headwidth=8)
    ax.annotate('', xy=(0.26, 0.52), xytext=(0.21, 0.52), arrowprops=arrow_props)
    ax.annotate('', xy=(0.47, 0.52), xytext=(0.42, 0.52), arrowprops=arrow_props)
    ax.annotate('', xy=(0.68, 0.52), xytext=(0.63, 0.52), arrowprops=arrow_props)
    ax.annotate('', xy=(0.89, 0.52), xytext=(0.84, 0.52), arrowprops=arrow_props)

    # Bitstream annotation below
    rect_bits = patches.FancyBboxPatch((0.68, 0.08), 0.16, 0.18, boxstyle="round,pad=0.02", fc=BG_COLOR, ec=EMERALD, lw=1.5)
    ax.add_patch(rect_bits)
    ax.text(0.76, 0.17, r"Binary Bitstream" "\n" r"(Transmitted / Stored)", color=EMERALD, fontsize=9.5, ha='center', va='center', fontweight='bold')
    ax.annotate('', xy=(0.76, 0.27), xytext=(0.76, 0.35), arrowprops=dict(arrowstyle='<->', color=EMERALD, lw=1.5))

    # Loss formulation header
    ax.text(0.58, 0.88, r"Rate-Distortion Optimization: $\mathcal{L} = \mathcal{R}(\hat{y}) + \lambda \mathcal{D}(x, \hat{x}) = \mathbb{E}[-\log_2 p_{\hat{y}}(\hat{y})] + \lambda \|x - \hat{x}\|^2$",
            color=TEXT_MAIN, fontsize=11.5, ha='center', fontweight='bold',
            bbox=dict(boxstyle="round,pad=0.4", fc=BG_COLOR, ec=BORDER_COLOR, lw=1.2))

    plt.suptitle("Modern Neural Compression: Pairing Non-Linear Transforms with Learned Entropy Models",
                 color=TEXT_MAIN, fontsize=14, fontweight='bold', y=0.98)
    save_fig(fig, "05_neural_compression.png")


if __name__ == "__main__":
    print("Generating clean mathematical diagrams...")
    create_fig1()
    create_fig2()
    create_fig3()
    create_fig4()
    create_fig5()
    print("All figures successfully generated!")
