import { createContext, useContext, useMemo, useState } from 'react';

const AIAssistantContext = createContext(null);

function createMessage(role, content, extra = {}) {
  return {
    id: `${role}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    role,
    content,
    timestamp: new Date().toISOString(),
    ...extra,
  };
}

export function AIAssistantProvider({ children }) {
  const [isOpen, setIsOpen] = useState(false);
  const [initialPrompt, setInitialPrompt] = useState('');
  const [assistantContext, setAssistantContext] = useState({});
  const [messages, setMessages] = useState([
    createMessage(
      'assistant',
      'Ask me anything about this analysis. I can explain charts, answer doubts, or generate a report from the data.'
    ),
  ]);

  const openAssistant = ({ prompt = '', context = {} } = {}) => {
    setAssistantContext(context || {});
    setInitialPrompt(prompt || '');
    setIsOpen(true);
  };

  const closeAssistant = () => {
    setIsOpen(false);
  };

  const resetConversation = () => {
    setMessages([
      createMessage(
        'assistant',
        'Ask me anything about this analysis. I can explain charts, answer doubts, or generate a report from the data.'
      ),
    ]);
    setAssistantContext({});
    setInitialPrompt('');
  };

  const value = useMemo(
    () => ({
      isOpen,
      initialPrompt,
      assistantContext,
      messages,
      setMessages,
      openAssistant,
      closeAssistant,
      resetConversation,
      createMessage,
    }),
    [assistantContext, initialPrompt, isOpen, messages]
  );

  return <AIAssistantContext.Provider value={value}>{children}</AIAssistantContext.Provider>;
}

export function useAIAssistant() {
  const context = useContext(AIAssistantContext);
  if (!context) {
    throw new Error('useAIAssistant must be used within an AIAssistantProvider');
  }
  return context;
}
