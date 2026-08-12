import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';
import { apiRequest } from '../../services/api';

export function Login() {
  const { setUser, setToken } = useAuth();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const handleLogin = async (e) => {
    e.preventDefault();
    try {
      setError('');
      const res = await apiRequest({
        method: 'post',
        url: '/api/auth/login',
        data: { email, password },
      });
      setToken(res.data.token);
      setUser(res.data.user);
      navigate('/');
    } catch (err) {
      setError(err.response?.data?.error || "Login failed");
    }
  };

  return (
    <div className="relative isolate flex min-h-screen w-full bg-white text-slate-900 font-display">
      <div className="relative z-20 flex flex-col w-full lg:w-[45%] xl:w-[40%] bg-white border-r border-slate-200 shadow-2xl p-12 lg:p-24 justify-center">
        <div className="flex items-center gap-3 mb-12">
          <div className="size-8 text-primary">
            <span className="material-symbols-outlined text-3xl font-black">finance_mode</span>
          </div>
          <h2 className="text-xl font-black tracking-tight uppercase">Civora</h2>
        </div>
        <h1 className="text-5xl font-black tracking-tighter mb-4">Sign in.</h1>
        <p className="text-slate-400 text-lg mb-10">Access your enterprise data hub.</p>
        {error && <div className="mb-6 p-4 bg-red-50 text-red-600 rounded-2xl text-sm font-bold">{error}</div>}
        <form onSubmit={handleLogin} className="relative z-10 space-y-6">
          <div className="space-y-2">
            <label className="text-xs font-black uppercase text-slate-400 tracking-widest">Email</label>
            <input
              className="relative z-10 block w-full appearance-none bg-slate-50 text-slate-900 caret-slate-900 border border-slate-100 rounded-2xl h-16 px-6 font-bold placeholder:text-slate-300 focus:border-primary/30 focus:bg-white focus:ring-4 focus:ring-primary/5 transition-all outline-none text-lg"
              value={email}
              onChange={e => setEmail(e.target.value)}
              type="email"
              autoComplete="email"
              placeholder="Enter your email"
              required
            />
          </div>
          <div className="space-y-2">
            <label className="text-xs font-black uppercase text-slate-400 tracking-widest">Password</label>
            <div className="relative">
              <input
                className="relative z-10 block w-full appearance-none bg-slate-50 text-slate-900 caret-slate-900 border border-slate-100 rounded-2xl h-16 pl-6 pr-16 font-bold placeholder:text-slate-300 focus:border-primary/30 focus:bg-white focus:ring-4 focus:ring-primary/5 transition-all outline-none text-lg"
                value={password}
                onChange={e => setPassword(e.target.value)}
                type={showPassword ? 'text' : 'password'}
                autoComplete="current-password"
                placeholder="Enter your password"
                required
              />
              <button
                type="button"
                onClick={() => setShowPassword((current) => !current)}
                className="absolute inset-y-0 right-0 z-20 flex w-16 items-center justify-center text-slate-400 transition-colors hover:text-primary"
                aria-label={showPassword ? 'Hide password' : 'Show password'}
                title={showPassword ? 'Hide password' : 'Show password'}
              >
                <span className="material-symbols-outlined text-[22px]">
                  {showPassword ? 'visibility_off' : 'visibility'}
                </span>
              </button>
            </div>
          </div>
          <button
            className="w-full bg-primary hover:bg-blue-700 text-white font-black text-xl h-16 rounded-2xl shadow-2xl shadow-primary/30 transition-all active:scale-95 mt-4"
            type="submit"
          >
            LOGIN
          </button>
        </form>
        <p className="mt-10 text-center font-bold text-slate-500">
          Need an account? <Link to="/signup" className="text-primary hover:underline">Register Hub</Link>
        </p>
      </div>
      <div className="pointer-events-none flex-1 bg-slate-50 relative overflow-hidden hidden lg:block">
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[120%] h-[120%] opacity-20">
          <div className="animate-wave-1 absolute inset-0 bg-blue-500 rounded-[40%] blur-[100px]"></div>
        </div>
        <div className="relative z-10 p-24 h-full flex flex-col justify-end">
          <div className="glass-panel p-12 rounded-[3.5rem] shadow-2xl max-w-lg animate-float">
            <div className="text-sm font-black text-primary uppercase tracking-[4px] mb-4">Precision First</div>
            <h2 className="text-4xl font-black tracking-tighter leading-tight">Predictive insights for high-growth teams.</h2>
          </div>
        </div>
      </div>
    </div>
  );
}
export default Login;
