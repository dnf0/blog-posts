# Rotating SSH System Blog Post Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write a technical blog post detailing the architecture, security benefits, and DevEx of a serverless rotating SSH Certificate Authority.

**Architecture:** The blog post will be written in MDX format, adhering strictly to the British English and structural guidelines defined in `GEMINI.md` and `BLOG_STYLE.md`. It follows "The Problem-Solution Narrative".

**Tech Stack:** Markdown (MDX), Python (for formatting validation).

---

### Task 1: Setup and Frontmatter

**Files:**
- Create: `content/2026-05-14-serverless-ssh-certificates.mdx`

- [ ] **Step 1: Create file and write Frontmatter**

```markdown
---
title: "Solving the Static SSH Key Nightmare with a Serverless CA"
date: "2026-05-14"
tags: ["security", "aws", "serverless", "ssh"]
description: "How we replaced static SSH keys with short-lived certificates using AWS Lambda and SSO, improving both security and developer experience."
---
*8 min read • Tags: `security`, `aws`, `serverless`, `ssh`*

```

- [ ] **Step 2: Commit**

```bash
git add content/2026-05-14-serverless-ssh-certificates.mdx
git commit -m "docs(blog): add frontmatter for serverless SSH post"
```

### Task 2: TL;DR and Introduction

**Files:**
- Modify: `content/2026-05-14-serverless-ssh-certificates.mdx`

- [ ] **Step 1: Write TL;DR and Introduction**

Append the following to the file:

```markdown
<Callout type="info">
**TL;DR:** We replaced traditional, permanent SSH public keys with short-lived (8-hour) SSH certificates issued by a serverless AWS Lambda function. By integrating with AWS SSO, we eliminated the need to manage static keys while actually improving the developer experience (DevEx) for tools like Cursor and VS Code.
</Callout>

The traditional way of handling SSH access to servers is fundamentally flawed. We generate a permanent SSH key pair, distribute the public key to a server, and hope the private key is never compromised. When an engineer leaves or a laptop is lost, revoking access becomes a frantic scramble across multiple systems. Worse, because public keys don't inherently support expiration, they tend to accumulate on servers indefinitely, creating a massive, unaudited attack surface.

It is a management nightmare that forces administrators to choose between rigorous security practices—which often slow down developers—and operational convenience. But it doesn't have to be this way. 

## Table of Contents
```

- [ ] **Step 2: Commit**

```bash
git add content/2026-05-14-serverless-ssh-certificates.mdx
git commit -m "docs(blog): add TLDR and introduction sections"
```

### Task 3: The Concept

**Files:**
- Modify: `content/2026-05-14-serverless-ssh-certificates.mdx`

- [ ] **Step 1: Write The Concept section**

Append the following to the file:

```markdown
## The Concept: Certificates vs. Public Keys

The solution to the static key problem has been built into OpenSSH for years: SSH Certificates.

Instead of configuring a server to trust a thousand individual public keys, you configure it to trust exactly one key: the Certificate Authority (CA). When a developer needs access, they present their public key to the CA. If the developer is authenticated, the CA cryptographically signs their public key, turning it into a time-bound "certificate."

This certificate contains metadata—such as the user's identity and, critically, an expiration time. The developer then presents this signed certificate to the target server. Because the server trusts the CA, it implicitly trusts any certificate signed by it, provided it hasn't expired.

This shifts the security burden from *managing keys on servers* to *securing the CA and the authentication process*.
```

- [ ] **Step 2: Commit**

```bash
git add content/2026-05-14-serverless-ssh-certificates.mdx
git commit -m "docs(blog): add concept explanation section"
```

### Task 4: Architecture Breakdown

**Files:**
- Modify: `content/2026-05-14-serverless-ssh-certificates.mdx`

- [ ] **Step 1: Write Architecture Breakdown section**

Append the following to the file:

```markdown
## Architecture Breakdown: A Serverless CA

To implement this securely without managing additional infrastructure, we built a serverless SSH Certificate Portal on AWS. 

Here is how the architecture flows:

1. **Authentication:** The developer logs in using their existing AWS SSO credentials via the AWS CLI. There is no separate portal login or password to manage.
2. **Request:** The developer invokes a specific AWS Lambda function (`cluster-ssh-sign-cert`), passing their public key as the payload.
3. **Verification:** The Lambda function, running under a strict IAM role, verifies the request's origin against AWS SSO and checks the user's public key against a registry in AWS Systems Manager (SSM) Parameter Store.
4. **Signing:** If valid, the Lambda function retrieves the CA private key securely stored in AWS Secrets Manager and uses an OpenSSH layer to sign the developer's public key.
5. **Issuance:** The function returns a certificate configured to expire in exactly 8 hours.

The target servers only need two lines in their `/etc/ssh/sshd_config` to enable this: `TrustedUserCAKeys` pointing to the CA's public key, and enabling RSA signatures (as our CA uses a 4096-bit RSA key).
```

- [ ] **Step 2: Commit**

```bash
git add content/2026-05-14-serverless-ssh-certificates.mdx
git commit -m "docs(blog): add serverless architecture breakdown"
```

### Task 5: Security & DevEx Wins

**Files:**
- Modify: `content/2026-05-14-serverless-ssh-certificates.mdx`

- [ ] **Step 1: Write Security & DevEx Wins section**

Append the following to the file:

```markdown
## Security & DevEx Wins

Security initiatives often encounter resistance because they introduce friction. The brilliance of this serverless CA approach is that it significantly enhances security while actually improving the developer experience.

### The Security Wins
- **Zero Static Credentials:** The 8-hour expiry means that even if a laptop is stolen, the credential becomes useless by the end of the day. There is no need for frantic revocation.
- **Single Point of Trust:** Administrators no longer distribute keys across fleets of servers. Trust is centralised at the CA.
- **Complete Auditability:** Because the signing process happens via an AWS Lambda function, every single certificate issuance is permanently logged in AWS CloudWatch.

### The Developer Experience (DevEx)
- **Self-Service:** Developers don't need to open IT tickets to get server access. They run a single script that uses their existing SSO session to grab a certificate instantly.
- **Seamless IDE Support:** Modern remote development relies heavily on SSH. This system works perfectly with tools like Cursor and VS Code; the IDE's remote extension simply uses the certificate loaded into the `ssh-agent`.
```

- [ ] **Step 2: Commit**

```bash
git add content/2026-05-14-serverless-ssh-certificates.mdx
git commit -m "docs(blog): add security and developer experience wins"
```

### Task 6: Conclusion and Wrap-up

**Files:**
- Modify: `content/2026-05-14-serverless-ssh-certificates.mdx`

- [ ] **Step 1: Write Conclusion, References, and Transparency Note**

Append the following to the file:

```markdown
## Conclusion

Moving from static SSH keys to short-lived certificates is a massive leap forward in infrastructure security. By leveraging serverless components like AWS Lambda, Secrets Manager, and SSO, we built a highly secure, zero-maintenance Certificate Authority that our engineering team actually enjoys using.

It is time to stop copying `id_rsa.pub` to `authorized_keys` and start treating infrastructure access as a dynamic, auditable event.

## References
- [AWS Secrets Manager Documentation](https://aws.amazon.com/secrets-manager/)
- [OpenSSH Certificates](https://www.openbsd.org/papers/bsdcan-sign.html)
- [AWS Systems Manager Parameter Store](https://aws.amazon.com/systems-manager/features/#Parameter_Store)

*Transparency Note: This blog post was written and structured with the assistance of an AI agent (Gemini).*
```

- [ ] **Step 2: Commit**

```bash
git add content/2026-05-14-serverless-ssh-certificates.mdx
git commit -m "docs(blog): add conclusion and references"
```

### Task 7: Validation

**Files:**
- Modify: None

- [ ] **Step 1: Run format validation script**

```bash
python scripts/validate_blog_format.py content/2026-05-14-serverless-ssh-certificates.mdx
```

Expected: The script should output a success message indicating the file adheres to all guidelines.

- [ ] **Step 2: Fix any issues (if script fails)**

If the script fails, identify the formatting error based on the output, apply the fix to `content/2026-05-14-serverless-ssh-certificates.mdx`, and re-run the validation script until it passes. Commit any fixes.
