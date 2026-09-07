"""
Generate publication-quality figures for the Neural Compression & Rate-Distortion blog post.
Follows strict dark slate palette, clear typography, and clean layouts.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np
import os

# Set style configuration
plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['figure.facecolor'] = '#0F172A'  # Slate 900
plt.rcParams['axes.facecolor'] = '#0F172A'
plt.rcParams['text.color'] = '#F8FAFC'        # Slate 50
plt.rcParams['axes.labelcolor'] = '#CBD5E1'    # Slate 300
plt.rcParams['xtick.color'] = '#94A3B8'        # Slate 400
plt.rcParams['ytick.color'] = '#94A3B8'
plt.rcParams['axes.edgecolor'] = '#334155'     # Slate 700

# Color palette
BG_DARK = '#0F172A'
CARD_BG = '#1E293B'
CARD_BORDER = '#334155'
TEXT_MAIN = '#F8FAFC'
TEXT_MUTED = '#94A3B8'
TEXT_ACCENT = '#38BDF8'  # Sky 400
CYAN = '#06B6D4'
PURPLE = '#A855F7'
GREEN = '#10B981'
AMBER = '#F59E0B'
ROSE = '#F43F5E'

OUT_DIR_PUBLIC = '/Users/danielfisher/repos/dnf0.github.io/public/images/neural-compression'
OUT_DIR_BLOG = '/Users/danielfisher/repos/blog-posts/content/images/neural-compression'
os.makedirs(OUT_DIR_PUBLIC, exist_ok=True)
os.makedirs(OUT_DIR_BLOG, exist_ok=True)

# -------------------------------------------------------------
# Figure 1: Lookup Table T vs. Neural Autoencoder
# -------------------------------------------------------------
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), dpi=300)

# Left: Classical Lookup Table
ax1.set_xlim(0, 10)
ax1.set_ylim(0, 10)
ax1.axis('off')
ax1.set_title("Classical Compression: Discrete Lookup Table T", fontsize=14, fontweight='bold', color=TEXT_MAIN, pad=15)

card1 = patches.FancyBboxPatch((0.5, 0.8), 9, 8.4, boxstyle="round,pad=0.2", facecolor=CARD_BG, edgecolor=CARD_BORDER, linewidth=1.5)
ax1.add_patch(card1)

ax1.text(5, 8.2, "Discrete Alphabet {x₁, x₂, ..., x_M}", ha='center', va='center', fontsize=12, fontweight='bold', color=TEXT_ACCENT)
ax1.text(5, 7.5, "Direct 1-to-1 Mapping in Memory", ha='center', va='center', fontsize=10, color=TEXT_MUTED)

# Draw Table Graphic
headers = ["Symbol xᵢ", "Code Key (Bits)", "Length bᵢ"]
cols_x = [2.2, 5.0, 7.8]
ax1.text(cols_x[0], 6.5, headers[0], ha='center', va='center', fontsize=11, fontweight='bold', color='#E2E8F0')
ax1.text(cols_x[1], 6.5, headers[1], ha='center', va='center', fontsize=11, fontweight='bold', color='#E2E8F0')
ax1.text(cols_x[2], 6.5, headers[2], ha='center', va='center', fontsize=11, fontweight='bold', color='#E2E8F0')

ax1.plot([1.0, 9.0], [6.1, 6.1], color=CARD_BORDER, lw=1.5)

rows = [
    ("x₁ (Sunny)", "0", "1 bit"),
    ("x₂ (Cloudy)", "10", "2 bits"),
    ("x₃ (Rainy)", "110", "3 bits"),
    ("x₄ (Snowy)", "111", "3 bits"),
]
for idx, (sym, code, length) in enumerate(rows):
    y = 5.3 - idx * 0.9
    ax1.text(cols_x[0], y, sym, ha='center', va='center', fontsize=10, color=CYAN)
    ax1.text(cols_x[1], y, f"`{code}`", ha='center', va='center', fontsize=10, color=GREEN, family='monospace', fontweight='bold')
    ax1.text(cols_x[2], y, length, ha='center', va='center', fontsize=10, color=TEXT_MUTED)

ax1.text(5, 1.4, "• Requires static, independent symbols\n• Scalability collapses on high-dimensional images (M → ∞)", 
         ha='center', va='center', fontsize=9.5, color=AMBER, bbox=dict(boxstyle="round,pad=0.4", facecolor="#1E293B", edgecolor=AMBER, alpha=0.4))


# Right: Neural Autoencoder
ax2.set_xlim(0, 10)
ax2.set_ylim(0, 10)
ax2.axis('off')
ax2.set_title("Neural Compression: Learned Non-linear Codec", fontsize=14, fontweight='bold', color=TEXT_MAIN, pad=15)

card2 = patches.FancyBboxPatch((0.5, 0.8), 9, 8.4, boxstyle="round,pad=0.2", facecolor=CARD_BG, edgecolor=CARD_BORDER, linewidth=1.5)
ax2.add_patch(card2)

ax2.text(5, 8.2, "Continuous High-Dim Signal x ∈ ℝⁿ", ha='center', va='center', fontsize=12, fontweight='bold', color=TEXT_ACCENT)
ax2.text(5, 7.5, "Non-linear Dimensionality Reduction & Quantization", ha='center', va='center', fontsize=10, color=TEXT_MUTED)

# Pipeline blocks
blocks = [
    ("Input x", 1.8, 5.0, 1.4, 1.2, CYAN),
    ("Encoder f_θ", 3.8, 5.0, 1.6, 1.2, PURPLE),
    ("Quantizer ⌊·⌉", 6.0, 5.0, 1.6, 1.2, AMBER),
    ("Decoder g_ϕ", 8.2, 5.0, 1.6, 1.2, GREEN),
]

for name, x, y, w, h, col in blocks:
    p = patches.FancyBboxPatch((x - w/2, y - h/2), w, h, boxstyle="round,pad=0.1", facecolor=col, alpha=0.15, edgecolor=col, linewidth=1.5)
    ax2.add_patch(p)
    ax2.text(x, y, name, ha='center', va='center', fontsize=9.5, fontweight='bold', color=col)

# Arrows
ax2.annotate("", xy=(2.9, 5.0), xytext=(2.6, 5.0), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))
ax2.annotate("", xy=(5.1, 5.0), xytext=(4.7, 5.0), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))
ax2.annotate("", xy=(7.3, 5.0), xytext=(6.9, 5.0), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))

ax2.text(5.0, 3.8, "Latents: z = f_θ(x)  →  Discretised: ẑ = ⌊z⌉", ha='center', va='center', fontsize=10, fontweight='bold', color=TEXT_MAIN)
ax2.text(5.0, 3.2, "Reconstruction: x' = g_ϕ(ẑ) ≈ x", ha='center', va='center', fontsize=10, color=GREEN)

ax2.text(5, 1.4, "• Learns non-linear spatial / spectral correlations\n• Scales to continuous multi-gigabyte satellite imagery", 
         ha='center', va='center', fontsize=9.5, color=GREEN, bbox=dict(boxstyle="round,pad=0.4", facecolor="#1E293B", edgecolor=GREEN, alpha=0.4))

plt.tight_layout()
fig.savefig(f"{OUT_DIR_PUBLIC}/01_lookup_to_neural.png", dpi=300, bbox_inches='tight', facecolor=BG_DARK)
fig.savefig(f"{OUT_DIR_BLOG}/01_lookup_to_neural.png", dpi=300, bbox_inches='tight', facecolor=BG_DARK)
plt.close()

# -------------------------------------------------------------
# Figure 2: Vector Quantized-VAE (VQ-VAE) Codebook Snapping
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
ax.set_facecolor(CARD_BG)
ax.set_xlim(-4, 4)
ax.set_ylim(-4, 4)

# Generate Voronoi / Codebook centroids
np.random.seed(42)
codebook = np.array([
    [-2.2, 1.8],
    [-1.8, -2.0],
    [0.0, 0.0],
    [2.2, 2.0],
    [2.0, -1.8]
])

# Draw codebook partition grid lines
for i in range(len(codebook)):
    for j in range(i + 1, len(codebook)):
        mid = (codebook[i] + codebook[j]) / 2.0
        diff = codebook[j] - codebook[i]
        norm = np.linalg.norm(diff)
        if norm < 4.0:
            perp = np.array([-diff[1], diff[0]]) / norm
            line_pts = np.array([mid - perp * 2.5, mid + perp * 2.5])
            ax.plot(line_pts[:, 0], line_pts[:, 1], color='#334155', linestyle='--', linewidth=1.0, alpha=0.6)

# Scatter continuous encodings
encodings = np.array([
    [-2.0, 2.2], [-2.5, 1.5], [-1.9, 1.2],
    [0.3, -0.4], [-0.2, 0.5], [0.5, 0.2],
    [2.5, 1.6], [1.8, 2.3], [2.1, -1.4], [1.6, -2.2]
])

# Draw vectors snapping to nearest codebook
for enc in encodings:
    dists = np.linalg.norm(codebook - enc, axis=1)
    nearest_idx = np.argmin(dists)
    nearest = codebook[nearest_idx]
    ax.annotate("", xy=nearest, xytext=enc, arrowprops=dict(arrowstyle="->", color='#F59E0B', lw=1.2, alpha=0.7))

ax.scatter(encodings[:, 0], encodings[:, 1], color=CYAN, s=50, label="Continuous Latents z = f(x)", zorder=4)
ax.scatter(codebook[:, 0], codebook[:, 1], color=ROSE, s=160, marker='D', edgecolor='#FFF', linewidth=1.5, label="Learned Codebook Centroids {z⁽ᵏ⁾}", zorder=5)

for idx, pt in enumerate(codebook):
    ax.text(pt[0] + 0.25, pt[1] + 0.25, f"e_{idx+1}", color=TEXT_MAIN, fontweight='bold', fontsize=11)

ax.set_title("Vector Quantization: Snapping Continuous Latent Space to K Codebook Centroids", fontsize=13, fontweight='bold', color=TEXT_MAIN, pad=12)
ax.set_xlabel("Latent Dimension 1 (z₁)", fontsize=11, color=TEXT_MUTED)
ax.set_ylabel("Latent Dimension 2 (z₂)", fontsize=11, color=TEXT_MUTED)
ax.grid(True, color='#334155', linestyle=':', alpha=0.5)
ax.legend(facecolor=CARD_BG, edgecolor=CARD_BORDER, labelcolor=TEXT_MAIN, loc='upper left')

# Annotation box
ax.text(0.02, 0.05, r"Quantization: $\hat{z} = \arg\min_k \|z - z^{(k)}\|_2$" + "\nDiscretises continuous manifold into discrete codebook index",
        transform=ax.transAxes, fontsize=10, color=TEXT_MAIN, bbox=dict(boxstyle="round,pad=0.4", facecolor=CARD_BG, edgecolor=TEXT_ACCENT, alpha=0.9))

plt.tight_layout()
fig.savefig(f"{OUT_DIR_PUBLIC}/02_vq_vae_codebook.png", dpi=300, bbox_inches='tight', facecolor=BG_DARK)
fig.savefig(f"{OUT_DIR_BLOG}/02_vq_vae_codebook.png", dpi=300, bbox_inches='tight', facecolor=BG_DARK)
plt.close()

# -------------------------------------------------------------
# Figure 3: Full End-to-End Lossy Codec Pipeline
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(13, 5.5), dpi=300)
ax.set_xlim(0, 13)
ax.set_ylim(0, 6)
ax.axis('off')

ax.set_title("End-to-End Lossy Neural Codec Pipeline: Signal -> Quantization -> Entropy Coding -> Reconstruction", 
             fontsize=13, fontweight='bold', color=TEXT_MAIN, pad=15)

# Outer Card
card = patches.FancyBboxPatch((0.4, 0.4), 12.2, 5.0, boxstyle="round,pad=0.2", facecolor=CARD_BG, edgecolor=CARD_BORDER, linewidth=1.5)
ax.add_patch(card)

# Nodes
stages = [
    ("Input Signal\nx ∈ Xⁿ", 1.5, 3.2, 1.6, 1.4, CYAN),
    ("Encoder e(x)\nFeature Map", 3.7, 3.2, 1.7, 1.4, PURPLE),
    ("Quantizer Q\nẑ ∈ Zᵐ", 5.9, 3.2, 1.6, 1.4, AMBER),
    ("Entropy Code ϕ\n(e.g., Arithmetic)", 8.1, 3.2, 1.8, 1.4, GREEN),
    ("Binary Bitstream\nLossless String", 10.5, 3.2, 1.8, 1.4, TEXT_ACCENT),
]

for title, x, y, w, h, col in stages:
    p = patches.FancyBboxPatch((x - w/2, y - h/2), w, h, boxstyle="round,pad=0.1", facecolor=col, alpha=0.15, edgecolor=col, linewidth=1.5)
    ax.add_patch(p)
    ax.text(x, y, title, ha='center', va='center', fontsize=9.5, fontweight='bold', color=col)

# Connecting arrows
ax.annotate("", xy=(2.7, 3.2), xytext=(2.4, 3.2), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))
ax.annotate("", xy=(4.9, 3.2), xytext=(4.6, 3.2), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))
ax.annotate("", xy=(7.0, 3.2), xytext=(6.8, 3.2), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))
ax.annotate("", xy=(9.4, 3.2), xytext=(9.1, 3.2), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))

# Decoder path below
ax.annotate("", xy=(10.5, 1.8), xytext=(10.5, 2.4), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))

dec_stages = [
    ("Entropy Decode ϕ⁻¹\nRecover ẑ", 8.1, 1.3, 1.8, 1.1, GREEN),
    ("Neural Decoder d(ẑ)\nReconstruct", 5.0, 1.3, 1.8, 1.1, PURPLE),
    ("Reconstructed x'\nDistortion D = ρ(x, x')", 1.8, 1.3, 2.0, 1.1, ROSE),
]

for title, x, y, w, h, col in dec_stages:
    p = patches.FancyBboxPatch((x - w/2, y - h/2), w, h, boxstyle="round,pad=0.1", facecolor=col, alpha=0.15, edgecolor=col, linewidth=1.5)
    ax.add_patch(p)
    ax.text(x, y, title, ha='center', va='center', fontsize=9.2, fontweight='bold', color=col)

ax.annotate("", xy=(9.2, 1.3), xytext=(9.6, 1.3), arrowprops=dict(arrowstyle="<-", color=TEXT_MUTED, lw=2))
ax.annotate("", xy=(6.1, 1.3), xytext=(7.1, 1.3), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))
ax.annotate("", xy=(3.0, 1.3), xytext=(4.0, 1.3), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))

# Lossy and Lossless callouts
ax.text(5.9, 4.3, "Lossy Stage: Info discarded during Discretisation", ha='center', va='center', fontsize=9, color=AMBER, fontweight='bold')
ax.text(9.3, 4.3, "Lossless Stage: Shannon Entropy Coded (Arithmetic)", ha='center', va='center', fontsize=9, color=GREEN, fontweight='bold')

plt.tight_layout()
fig.savefig(f"{OUT_DIR_PUBLIC}/03_lossy_compression_pipeline.png", dpi=300, bbox_inches='tight', facecolor=BG_DARK)
fig.savefig(f"{OUT_DIR_BLOG}/03_lossy_compression_pipeline.png", dpi=300, bbox_inches='tight', facecolor=BG_DARK)
plt.close()

# -------------------------------------------------------------
# Figure 4: Rate-Distortion Optimization Curve (Pareto Frontier)
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
ax.set_facecolor(CARD_BG)

# Simulated R-D Curve
rate = np.linspace(0.2, 4.0, 200)
distortion = 1.0 / (rate ** 1.3) + 0.05

ax.plot(rate, distortion, color=CYAN, linewidth=3, label="Optimal Rate-Distortion Frontier (Pareto Bound)")

# Unachievable vs Suboptimal zones
ax.fill_between(rate, distortion, 4.0, color=CARD_BORDER, alpha=0.2, label="Suboptimal Operational Region")
ax.fill_between(rate, 0, distortion, color=ROSE, alpha=0.08, label="Information-Theoretic Infeasible Region")

# Specific operational points for different lambdas
pts_rate = [0.5, 1.2, 2.5]
pts_dist = [1.0 / (r ** 1.3) + 0.05 for r in pts_rate]
labels = [
    r"Low Bitrate ($\lambda_{small}$): High Compression, Blur",
    r"Balanced ($\lambda_{mid}$): Standard Web / Archive",
    r"High Fidelity ($\lambda_{large}$): Scientific Analytics"
]
colors = [AMBER, GREEN, PURPLE]

for r, d, lbl, col in zip(pts_rate, pts_dist, labels, colors):
    ax.scatter(r, d, color=col, s=120, zorder=5, edgecolor='#FFF', linewidth=1.5)
    ax.text(r + 0.1, d + 0.15, lbl, color=TEXT_MAIN, fontsize=9.5, fontweight='bold')

# Tangent line indicating lambda slope
r_tan = 1.2
d_tan = 1.0 / (r_tan ** 1.3) + 0.05
slope = -1.3 * (r_tan ** -2.3)
x_vals = np.linspace(0.6, 1.8, 50)
y_vals = d_tan + slope * (x_vals - r_tan)
ax.plot(x_vals, y_vals, color=AMBER, linestyle='--', linewidth=1.5, label=r"Lagrangian Tangent: Slope = $-\frac{1}{\lambda}$")

ax.set_xlim(0, 4.2)
ax.set_ylim(0, 3.2)
ax.set_title(r"Rate-Distortion Optimization: $\min \mathcal{L} = \lambda D + R$", fontsize=13, fontweight='bold', color=TEXT_MAIN, pad=12)
ax.set_xlabel("Rate R (Bits per pixel / symbol) → [Lower is Better]", fontsize=11, color=TEXT_MUTED)
ax.set_ylabel("Distortion D = ρ(x, x') → [Lower is Better]", fontsize=11, color=TEXT_MUTED)
ax.grid(True, color='#334155', linestyle=':', alpha=0.5)
ax.legend(facecolor=CARD_BG, edgecolor=CARD_BORDER, labelcolor=TEXT_MAIN, loc='upper right')

# Annotation of Lagrangian objective
ax.text(0.04, 0.15, r"$\min \lambda D + R$" + "\n• " + r"$\lambda \to 0$: Minimise bitrate (tiny files)" + "\n• " + r"$\lambda \to \infty$: Minimise error (lossless limit)",
        transform=ax.transAxes, fontsize=10, color=TEXT_MAIN, bbox=dict(boxstyle="round,pad=0.4", facecolor=CARD_BG, edgecolor=CYAN, alpha=0.9))

# -------------------------------------------------------------
# Figure 5: The Transform Coding Paradigm (Analysis & Synthesis)
# -------------------------------------------------------------
fig, ax = plt.subplots(figsize=(13, 6.5), dpi=300)
ax.set_xlim(0, 13)
ax.set_ylim(0, 7)
ax.axis('off')

ax.set_title("The Transform Coding Framework: Non-Linear Analysis & Synthesis with Learned Entropy Modeling", 
             fontsize=13, fontweight='bold', color=TEXT_MAIN, pad=15)

# Outer Card
card = patches.FancyBboxPatch((0.4, 0.4), 12.2, 6.1, boxstyle="round,pad=0.2", facecolor=CARD_BG, edgecolor=CARD_BORDER, linewidth=1.5)
ax.add_patch(card)

# Top row: Encoder / Transmitter
# Input x -> Analysis Transform f -> Latents z -> Quantizer q -> z_hat -> Entropy Coder phi -> Bitstream
ax.text(0.8, 6.0, "Transmitter / Encoding Side:", fontsize=11, fontweight='bold', color=TEXT_ACCENT)

nodes_top = [
    ("Signal x\n(Correlated)", 1.5, 4.8, 1.6, 1.2, CYAN),
    ("Analysis f_θ\n(Decorrelation)", 3.8, 4.8, 1.8, 1.2, PURPLE),
    ("Continuous z\n(Latent Space)", 6.1, 4.8, 1.7, 1.2, TEXT_MAIN),
    ("Quantizer q\n(Information Loss)", 8.4, 4.8, 1.8, 1.2, AMBER),
    ("Discrete ẑ\n(Symbols)", 10.7, 4.8, 1.6, 1.2, GREEN),
]

for title, x, y, w, h, col in nodes_top:
    p = patches.FancyBboxPatch((x - w/2, y - h/2), w, h, boxstyle="round,pad=0.1", facecolor=col, alpha=0.15, edgecolor=col, linewidth=1.5)
    ax.add_patch(p)
    ax.text(x, y, title, ha='center', va='center', fontsize=9.2, fontweight='bold', color=col)

ax.annotate("", xy=(2.7, 4.8), xytext=(2.4, 4.8), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))
ax.annotate("", xy=(5.1, 4.8), xytext=(4.8, 4.8), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))
ax.annotate("", xy=(7.3, 4.8), xytext=(7.1, 4.8), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))
ax.annotate("", xy=(9.7, 4.8), xytext=(9.4, 4.8), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))

# Entropy Model p'(z_hat) and Bitstream
p_ent = patches.FancyBboxPatch((10.7 - 0.9, 3.2 - 0.4), 1.8, 0.8, boxstyle="round,pad=0.1", facecolor=GREEN, alpha=0.25, edgecolor=GREEN, linewidth=1.5)
ax.add_patch(p_ent)
ax.text(10.7, 3.2, "Entropy Model p'\nRate: −log₂ p'(ẑ)", ha='center', va='center', fontsize=8.5, fontweight='bold', color=GREEN)

ax.annotate("", xy=(10.7, 3.7), xytext=(10.7, 4.1), arrowprops=dict(arrowstyle="->", color=GREEN, lw=1.5))
ax.annotate("", xy=(10.7, 2.3), xytext=(10.7, 2.7), arrowprops=dict(arrowstyle="->", color=GREEN, lw=1.5))

# Bottom row: Receiver / Decoding Side
# Bitstream -> Discrete z_hat -> Synthesis Transform g_phi -> Reconstructed x'
ax.text(0.8, 2.7, "Receiver / Decoding Side:", fontsize=11, fontweight='bold', color=TEXT_ACCENT)

nodes_bot = [
    ("Reconstructed x'\nsubject to loss", 1.5, 1.6, 1.8, 1.2, ROSE),
    ("Synthesis g_ϕ\n(Reconstruction)", 4.5, 1.6, 2.0, 1.2, PURPLE),
    ("Discrete ẑ\n(Recovered)", 8.0, 1.6, 1.8, 1.2, GREEN),
    ("Bitstream String\nLossless Buffer", 10.7, 1.6, 1.8, 1.2, TEXT_ACCENT),
]

for title, x, y, w, h, col in nodes_bot:
    p = patches.FancyBboxPatch((x - w/2, y - h/2), w, h, boxstyle="round,pad=0.1", facecolor=col, alpha=0.15, edgecolor=col, linewidth=1.5)
    ax.add_patch(p)
    ax.text(x, y, title, ha='center', va='center', fontsize=9.2, fontweight='bold', color=col)

ax.annotate("", xy=(9.7, 1.6), xytext=(9.0, 1.6), arrowprops=dict(arrowstyle="<-", color=TEXT_MUTED, lw=2))
ax.annotate("", xy=(5.7, 1.6), xytext=(6.9, 1.6), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))
ax.annotate("", xy=(2.5, 1.6), xytext=(3.3, 1.6), arrowprops=dict(arrowstyle="->", color=TEXT_MUTED, lw=2))

# Composite Loss Callout Box at bottom left/center
ax.text(5.5, 3.2, r"$\mathcal{L}(x) = \lambda \cdot D(x, g_\phi(q(f_\theta(x)))) + R(q(f_\theta(x)))$" + "\n" + r"$\mathcal{L}(x) = \lambda \cdot \rho(x, x') - \log_2 p'(ẑ)$",
        ha='center', va='center', fontsize=11, color=TEXT_MAIN, bbox=dict(boxstyle="round,pad=0.4", facecolor="#1E293B", edgecolor=TEXT_ACCENT, linewidth=1.5))

plt.tight_layout()
fig.savefig(f"{OUT_DIR_PUBLIC}/05_transform_coding_paradigm.png", dpi=300, bbox_inches='tight', facecolor=BG_DARK)
fig.savefig(f"{OUT_DIR_BLOG}/05_transform_coding_paradigm.png", dpi=300, bbox_inches='tight', facecolor=BG_DARK)
plt.close()

print("All 5 figures generated successfully at 300 DPI!")

