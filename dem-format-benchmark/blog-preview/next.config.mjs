import createMDX from '@next/mdx'
import remarkFrontmatter from 'remark-frontmatter'
import remarkMath from 'remark-math'
import remarkGfm from 'remark-gfm'
import rehypeKatex from 'rehype-katex'

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'export',
  pageExtensions: ['js', 'jsx', 'md', 'mdx', 'ts', 'tsx'],
}

const withMDX = createMDX({
  extension: /\.mdx?$/,
  options: {
    remarkPlugins: [remarkFrontmatter, remarkGfm, remarkMath],
    rehypePlugins: [rehypeKatex],
  },
})

export default withMDX(nextConfig)
