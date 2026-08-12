import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { apiRequest } from '../../services/api';

export function Signup() {
  const [name, setName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const handleSignup = async (e) => {
    e.preventDefault();
    try {
      setError('');
      await apiRequest({
        method: 'post',
        url: '/api/auth/register',
        data: { name, email, password },
      });
      alert('Registration was successful. Redirecting to the login page.');
      navigate('/login');
    } catch (err) {
      const reason = err.response?.data?.error || err.message || 'Registration failed';
      setError(reason);
      alert(`Registration failed: ${reason}`);
    }
  };

  return (
    <div className="relative isolate flex min-h-screen w-full bg-white text-slate-900 font-display">
      <div className="relative z-20 flex flex-col w-full lg:w-[45%] xl:w-[40%] bg-white border-r border-slate-200 shadow-2xl p-12 lg:p-24 justify-center">
        <h1 className="text-5xl font-black tracking-tighter mb-4 text-primary">Join.</h1>
        <p className="text-slate-400 text-lg mb-10">Create your unified dashboard profile.</p>
        {error && <div className="mb-6 p-4 bg-red-50 text-red-600 rounded-2xl text-sm font-bold">{error}</div>}
        <form onSubmit={handleSignup} className="space-y-6">
          <div className="space-y-2">
            <label className="text-xs font-black uppercase text-slate-400">NAME</label>
            <input
              className="relative z-10 block w-full appearance-none bg-slate-50 text-slate-900 caret-slate-900 border border-slate-100 rounded-2xl h-16 px-6 font-bold placeholder:text-slate-300 outline-none"
              value={name}
              onChange={e => setName(e.target.value)}
              type="text"
              autoComplete="name"
              placeholder="Your name"
              required
            />
          </div>
          <div className="space-y-2">
            <label className="text-xs font-black uppercase text-slate-400">EMAIL</label>
            <input
              className="relative z-10 block w-full appearance-none bg-slate-50 text-slate-900 caret-slate-900 border border-slate-100 rounded-2xl h-16 px-6 font-bold placeholder:text-slate-300 outline-none"
              value={email}
              onChange={e => setEmail(e.target.value)}
              type="email"
              autoComplete="email"
              placeholder="Your email"
              required
            />
          </div>
          <div className="space-y-2">
            <label className="text-xs font-black uppercase text-slate-400">PASSWORD</label>
            <input
              className="relative z-10 block w-full appearance-none bg-slate-50 text-slate-900 caret-slate-900 border border-slate-100 rounded-2xl h-16 px-6 font-bold placeholder:text-slate-300 outline-none"
              value={password}
              onChange={e => setPassword(e.target.value)}
              type="password"
              autoComplete="new-password"
              placeholder="Create a password"
              required
            />
          </div>
          <button className="w-full bg-primary text-white font-black h-16 rounded-2xl text-xl mt-4" type="submit">
            CREATE HUB
          </button>
        </form>
        <p className="mt-8 text-center font-bold text-slate-500">
          Already a member? <Link to="/login" className="text-primary hover:underline">Log In</Link>
        </p>
      </div>
      <div className="pointer-events-none flex-1 bg-primary text-white p-24 flex flex-col justify-center items-center text-center">
        <div className="text-[15rem] font-black opacity-10 absolute select-none">DATA</div>
        <h2 className="text-6xl font-black tracking-tighter relative">Empower your strategy with AI.</h2>
      </div>
    </div>
  );
}
export default Signup;
