export function useMDXComponents(components) {
  return {
    ...components,
    Callout: ({ children }) => <div style={{ padding: '1rem', backgroundColor: '#e0f2fe', borderRadius: '0.5rem', margin: '1rem 0' }}>{children}</div>,
  }
}
