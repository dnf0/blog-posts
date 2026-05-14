import 'katex/dist/katex.min.css'
import './globals.css'
import { Inter } from 'next/font/google'

const inter = Inter({ subsets: ['latin'] })

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <main className="container">
          {children}
        </main>
      </body>
    </html>
  )
}
