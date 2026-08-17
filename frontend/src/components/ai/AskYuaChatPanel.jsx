import { useEffect, useRef, useState } from 'react';
import { useAuth } from '../../context/AuthContext';
import { useAIAssistant } from '../../context/AIAssistantContext';
import { sendAIAssistantMessage } from '../../services/aiService';

function buildChatPayload(messages, assistantContext) {
  return {
    context: assistantContext,
    messages: messages
      .filter((message) => message.role === 'user' || message.role === 'assistant')
      .map((message) => ({
        role: message.role,
        content: message.content,
      })),
  };
}

export function AskYuaChatPanel() {
  const { token } = useAuth();
  const {
    isOpen,
    initialPrompt,
    assistantContext,
    messages,
    setMessages,
    closeAssistant,
    resetConversation,
    createMessage,
  } = useAIAssistant();
  const [inputValue, setInputValue] = useState('');
  const [isSending, setIsSending] = useState(false);
  const [sendError, setSendError] = useState('');
  const [connectionStatus, setConnectionStatus] = useState('checking');
  const messagesEndRef = useRef(null);
  const textareaRef = useRef(null);

  useEffect(() => {
    if (!isOpen) {
      return undefined;
    }

    setInputValue(initialPrompt || '');
    setSendError('');
    setConnectionStatus('checking');

    const timer = window.setTimeout(() => {
      textareaRef.current?.focus();
    }, 50);

    return () => window.clearTimeout(timer);
  }, [initialPrompt, isOpen]);

  useEffect(() => {
    if (!isOpen) {
      return undefined;
    }

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    const handleKeyDown = (event) => {
      if (event.key === 'Escape') {
        closeAssistant();
      }
    };

    window.addEventListener('keydown', handleKeyDown);

    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [closeAssistant, isOpen]);

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
  }, [isOpen, messages, isSending]);

  const handleSend = async (overrideText) => {
    const text = String(overrideText ?? inputValue).trim();
    if (!text || isSending) return;

    const userMessage = createMessage('user', text);
    const placeholderMessage = createMessage('assistant', 'Thinking...', { isPlaceholder: true });
    const nextMessages = [...messages, userMessage, placeholderMessage];

    setMessages(nextMessages);
    setInputValue('');
    setIsSending(true);
    setSendError('');

    try {
      const response = await sendAIAssistantMessage(buildChatPayload([...messages, userMessage], assistantContext), token);
      const assistantReply =
        response.data?.reply || response.data?.message || response.data?.answer || 'I could not generate a response.';
      setConnectionStatus(response.data?.source === 'watsonx-orchestrate' ? 'connected' : 'checking');

      setMessages((currentMessages) =>
        currentMessages.map((message) =>
          message.id === placeholderMessage.id
            ? {
                ...message,
                content: assistantReply,
                isPlaceholder: false,
                source: response.data?.source || 'watsonx-orchestrate',
                context_summary: response.data?.context_summary,
              }
            : message
        )
      );
    } catch (error) {
      const errorMessage =
        error.response?.data?.error ||
        error.response?.data?.message ||
        error.message ||
        'Sorry, I could not reach the AI service.';

      setSendError(errorMessage);
      setConnectionStatus('unavailable');
      setMessages((currentMessages) =>
        currentMessages.map((message) =>
          message.id === placeholderMessage.id
            ? {
                ...message,
                content: errorMessage,
                isPlaceholder: false,
                role: 'assistant',
                isError: true,
              }
            : message
        )
      );
    } finally {
      setIsSending(false);
    }
  };

  const handleKeyDown = (event) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      handleSend();
    }
  };

  const handleReset = () => {
    setInputValue('');
    setSendError('');
    setConnectionStatus('checking');
    resetConversation();
  };

  const starterPrompts = [
    'Explain this analysis in simple terms.',
    'Generate a concise report from the current data.',
    'What are the main risks and opportunities?',
  ];

  if (!isOpen) {
    return null;
  }

  const connectionBadge =
    connectionStatus === 'connected'
      ? {
          label: 'IBM connected',
          className:
            'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900/40 dark:bg-emerald-950/30 dark:text-emerald-300',
        }
      : connectionStatus === 'unavailable'
      ? {
          label: 'IBM unavailable',
          className: 'border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-900/40 dark:bg-rose-950/30 dark:text-rose-300',
        }
      : {
          label: 'IBM checking',
          className: 'border-amber-200 bg-amber-50 text-amber-700 dark:border-amber-900/40 dark:bg-amber-950/30 dark:text-amber-300',
        };

  return (
    <>
      <div
        className={`fixed inset-0 z-[90] bg-slate-950/65 backdrop-blur-xl transition-opacity duration-300 ${
          isOpen ? 'opacity-100 pointer-events-auto' : 'opacity-0 pointer-events-none'
        }`}
        onClick={closeAssistant}
      />

      <aside
        className={`fixed inset-0 z-[100] flex h-full w-full flex-col overflow-hidden bg-[radial-gradient(circle_at_top_left,_rgba(129,140,248,0.16),_transparent_35%),linear-gradient(180deg,_rgba(248,250,252,0.98)_0%,_rgba(241,245,249,0.96)_48%,_rgba(226,232,240,0.92)_100%)] text-slate-900 transition-all duration-300 dark:bg-[radial-gradient(circle_at_top_left,_rgba(79,70,229,0.24),_transparent_34%),linear-gradient(180deg,_rgba(2,6,23,0.98)_0%,_rgba(15,23,42,0.96)_48%,_rgba(2,6,23,0.94)_100%)] dark:text-slate-100 ${
          isOpen ? 'opacity-100 pointer-events-auto' : 'opacity-0 pointer-events-none'
        }`}
        aria-hidden={!isOpen}
        role="dialog"
        aria-modal="true"
        aria-label="Ask Yua AI"
      >
        <div className="pointer-events-none absolute inset-0 overflow-hidden">
          <div className="absolute -left-24 top-0 h-72 w-72 rounded-full bg-sky-400/20 blur-3xl" />
          <div className="absolute right-8 top-24 h-80 w-80 rounded-full bg-indigo-500/20 blur-3xl" />
          <div className="absolute bottom-0 left-1/4 h-64 w-64 rounded-full bg-cyan-400/10 blur-3xl" />
        </div>

        <div className="relative z-10 flex h-full min-h-0 flex-col px-4 py-4 sm:px-6 sm:py-6 lg:px-8 lg:py-8">
          <div className="mx-auto flex w-full max-w-7xl flex-1 min-h-0 flex-col overflow-hidden rounded-[2rem] border border-white/50 bg-white/85 shadow-[0_30px_100px_rgba(15,23,42,0.22)] backdrop-blur-2xl dark:border-slate-700/60 dark:bg-slate-950/75">
            <div className="flex items-center justify-between border-b border-slate-200/80 px-5 py-4 sm:px-6 dark:border-slate-800">
              <div className="flex items-center gap-3">
                <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-indigo-600 text-white shadow-lg shadow-indigo-600/20">
                  <span className="material-symbols-outlined">auto_awesome</span>
                </div>
                <div>
                  <div className="text-sm font-black tracking-tight text-slate-900 dark:text-white">Ask Yua AI</div>
                  <div className="text-[10px] font-black uppercase tracking-[0.24em] text-slate-400">
                    Backend API key integration
                  </div>
                </div>
              </div>

              <div className="flex items-center gap-2">
                <div
                  className={`rounded-full border px-4 py-2 text-[10px] font-black uppercase tracking-[0.24em] ${connectionBadge.className}`}
                >
                  {connectionBadge.label}
                </div>
                <button
                  type="button"
                  onClick={handleReset}
                  className="rounded-full border border-slate-200 bg-white px-4 py-2 text-[10px] font-black uppercase tracking-[0.24em] text-slate-500 transition-colors hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300 dark:hover:bg-slate-800"
                >
                  Reset
                </button>
                <button
                  type="button"
                  onClick={closeAssistant}
                  className="rounded-full p-2 text-slate-400 transition-colors hover:bg-slate-100 hover:text-slate-900 dark:hover:bg-slate-800 dark:hover:text-white"
                  aria-label="Close chat panel"
                >
                  <span className="material-symbols-outlined text-[28px]">close</span>
                </button>
              </div>
            </div>

            <div className="grid flex-1 min-h-0 gap-0 lg:grid-cols-[300px_minmax(0,1fr)]">
              <div className="border-b border-slate-200/80 bg-slate-50/90 px-5 py-5 dark:border-slate-800 dark:bg-slate-900/40 lg:border-b-0 lg:border-r">
                <div className="rounded-[1.5rem] border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-800 dark:bg-slate-950/60">
                  <div className="text-[10px] font-black uppercase tracking-[0.24em] text-slate-400">Quick prompts</div>
                  <div className="mt-4 flex flex-col gap-3">
                    {starterPrompts.map((prompt) => (
                      <button
                        key={prompt}
                        type="button"
                        onClick={() => handleSend(prompt)}
                        disabled={isSending}
                        className="rounded-2xl border border-indigo-100 bg-indigo-50 px-4 py-3 text-left text-sm font-bold text-indigo-700 transition-all hover:-translate-y-0.5 hover:bg-indigo-100 disabled:cursor-not-allowed disabled:opacity-50 dark:border-indigo-900/30 dark:bg-indigo-900/20 dark:text-indigo-200 dark:hover:bg-indigo-900/30"
                      >
                        {prompt}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="mt-4 rounded-[1.5rem] border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-800 dark:bg-slate-950/60">
                  <div className="text-[10px] font-black uppercase tracking-[0.24em] text-slate-400">Active context</div>
                  <div className="mt-3 text-sm font-semibold text-slate-600 dark:text-slate-300">
                    <div>{assistantContext.page || 'Analysis'}</div>
                    <div className="mt-1 text-xs font-bold uppercase tracking-[0.18em] text-slate-400">
                      {assistantContext.year ? `Year ${assistantContext.year}` : 'No year selected'}
                    </div>
                  </div>
                </div>
              </div>

              <div className="flex min-h-0 flex-col">
                <div className="border-b border-slate-200/80 px-5 py-4 dark:border-slate-800 sm:px-6">
                  <div className="flex flex-wrap items-center gap-3">
                    <div className="rounded-full border border-emerald-200 bg-emerald-50 px-4 py-2 text-[11px] font-black uppercase tracking-[0.22em] text-emerald-700 dark:border-emerald-900/40 dark:bg-emerald-950/30 dark:text-emerald-300">
                      Full Screen Chat
                    </div>
                    <div className="text-sm font-medium text-slate-500 dark:text-slate-300">
                      Ask about metrics, charts, trends, reports, or business summaries.
                    </div>
                  </div>
                </div>

                <div className="flex-1 min-h-0 overflow-y-auto px-4 py-5 sm:px-6">
                  <div className="space-y-4">
                    {messages.map((message) => (
                      <div
                        key={message.id}
                        className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'}`}
                      >
                        <div
                          className={`max-w-[min(92%,760px)] rounded-[1.75rem] px-4 py-3 text-sm leading-relaxed shadow-sm sm:px-5 sm:py-4 ${
                            message.role === 'user'
                              ? 'bg-slate-900 text-white dark:bg-white dark:text-slate-900'
                              : message.isError
                              ? 'border border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-900/30 dark:bg-rose-950/20 dark:text-rose-200'
                              : 'border border-slate-200 bg-white text-slate-700 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-200'
                          }`}
                        >
                          <div className="whitespace-pre-wrap">{message.content}</div>
                          {message.source && (
                            <div className="mt-2 text-[10px] font-black uppercase tracking-[0.2em] opacity-60">
                              {message.source}
                            </div>
                          )}
                        </div>
                      </div>
                    ))}
                    {isSending && (
                      <div className="flex justify-start">
                        <div className="rounded-[1.75rem] border border-slate-200 bg-white px-4 py-3 text-sm text-slate-500 dark:border-slate-800 dark:bg-slate-900 dark:text-slate-300">
                          Thinking...
                        </div>
                      </div>
                    )}
                    <div ref={messagesEndRef} />
                  </div>
                </div>

                <div className="border-t border-slate-200/80 bg-white/90 p-4 dark:border-slate-800 dark:bg-slate-950/70 sm:p-5">
                  <div className="mx-auto max-w-5xl">
                    <textarea
                      ref={textareaRef}
                      value={inputValue}
                      onChange={(event) => setInputValue(event.target.value)}
                      onKeyDown={handleKeyDown}
                      rows={4}
                      placeholder="Ask anything about the analysis, request a report, or describe a doubt..."
                      className="w-full resize-none rounded-[1.75rem] border border-slate-200 bg-slate-50 px-4 py-4 text-sm outline-none transition-all placeholder:text-slate-400 focus:border-indigo-400 focus:ring-4 focus:ring-indigo-100 dark:border-slate-700 dark:bg-slate-900/80 dark:text-slate-100 dark:placeholder:text-slate-500 dark:focus:border-indigo-700 dark:focus:ring-indigo-950/30"
                    />
                    {sendError ? (
                      <div className="mt-3 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 dark:border-rose-900/30 dark:bg-rose-950/20 dark:text-rose-200">
                        {sendError}
                      </div>
                    ) : null}
                    <div className="mt-3 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                      <div className="text-[10px] font-black uppercase tracking-[0.22em] text-slate-400">
                        Press Enter to send
                      </div>
                      <button
                        type="button"
                        onClick={() => handleSend()}
                        disabled={isSending || !inputValue.trim()}
                        className="inline-flex items-center justify-center gap-2 rounded-2xl bg-indigo-600 px-5 py-3 text-sm font-black text-white shadow-lg shadow-indigo-600/20 transition-all hover:-translate-y-0.5 hover:bg-indigo-500 disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0"
                      >
                        <span className="material-symbols-outlined text-sm">send</span>
                        Send
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </aside>
    </>
  );
}

export default AskYuaChatPanel;
