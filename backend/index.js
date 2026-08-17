const express = require('express');
const cors = require('cors');
const { ensureSchema } = require('./db');
const authRouter = require('./routes/auth');
const uploadRouter = require('./routes/upload');
const projectsRouter = require('./routes/projects');
const dashboardRouter = require('./routes/dashboard');
const aiRouter = require('./routes/ai');
const userRouter = require('./routes/user');
const notificationsRouter = require('./routes/notifications');

const app = express();
const port = process.env.PORT || 8000;

app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
  res.send('Civora Auth & Data API');
});

app.use('/api/auth', authRouter);
app.use('/api', uploadRouter);
app.use('/api', projectsRouter);
app.use('/api', dashboardRouter);
app.use('/api', aiRouter);
app.use('/api', userRouter);
app.use('/api', notificationsRouter);

ensureSchema()
  .then(() => {
    app.listen(port, '0.0.0.0', () => {
      console.log(`Backend listening at http://0.0.0.0:${port}`);
    });
  })
  .catch((error) => {
    console.error('Failed to ensure database schema:', error);
    process.exit(1);
  });
