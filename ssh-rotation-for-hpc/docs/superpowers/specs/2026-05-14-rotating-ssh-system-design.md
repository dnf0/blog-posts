# Design Specification: Rotating SSH System Blog Post

## Overview
A technical blog post detailing the architecture, security benefits, and developer experience (DevEx) of replacing static SSH keys with a serverless, rotating SSH Certificate Authority (CA). The post is based on the system implemented in the `surge-scripts` repository.

## Target Audience & Angle
- **Audience:** General Developers interested in serverless patterns, security, or SSH infrastructure.
- **Angle:** Security-focused via "The Problem-Solution Narrative". It bridges the gap between the common developer pain point of static SSH key management and a practical serverless security pattern.

## Content Structure
The post will strictly adhere to the formatting guidelines defined in `GEMINI.md` and `BLOG_STYLE.md` (British English spelling, required sections).

1. **Frontmatter & Metadata**
   - YAML frontmatter with `title`, `date`, `tags` (e.g., `security`, `aws`, `serverless`), and `description`.
   - Formatted read time and tags line.
2. **TL;DR Callout**
   - A concise info block summarising how serverless, short-lived (8-hour) SSH certificates solve the security nightmare of static keys.
3. **Introduction: The Static Key Nightmare**
   - The problem statement: lost keys, lack of expiration, difficult revocation, and the dangers of key sharing.
4. **Table of Contents**
   - Auto-generated markdown links.
5. **The Concept: Certificates vs. Public Keys**
   - A brief, educational primer explaining how an SSH CA works differently from traditional public key authentication.
6. **Architecture Breakdown: A Serverless CA**
   - Exploring the `surge-scripts` implementation.
   - Flow: Developer uses AWS SSO -> Invokes AWS Lambda -> Lambda signs using CA key in Secrets Manager -> Developer connects.
7. **Security & DevEx Wins**
   - **Security:** 8-hour credential lifespans, single point of trust, CloudWatch auditability.
   - **DevEx:** Self-service issuance, automatic public key registration via AWS SSM, and seamless support for remote IDEs like Cursor/VS Code.
8. **Conclusion**
   - The inevitable industry shift from static credentials to dynamic, zero-trust access.
9. **References**
   - Links to relevant concepts (AWS SSO, SSH Certificates).
10. **Transparency Note**
    - The standard AI assistance footer.

## Self-Review Checklist Status
- [x] Placeholder scan: No "TBD" or "TODO" items remain.
- [x] Internal consistency: The architecture section aligns with the DevEx wins described.
- [x] Scope check: The scope is appropriately focused on a single blog post.
- [x] Ambiguity check: The narrative approach is explicitly stated and structured.
