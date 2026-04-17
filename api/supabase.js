// api/supabase.js — Proxy server-side para o Supabase
// A service_role key fica segura no servidor (variável de ambiente Vercel).
// O browser nunca vê essa chave — só chama /api/supabase?p=<path>

export default async function handler(req, res) {
  if (req.method === 'OPTIONS') return res.status(200).end();

  const SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
  if (!SERVICE_KEY) {
    return res.status(500).json({ error: 'SUPABASE_SERVICE_KEY não configurada no Vercel.' });
  }

  const supaPath = req.query.p;
  if (!supaPath) return res.status(400).json({ error: 'Parâmetro p ausente.' });

  const url = `https://ugvmbtufwtpfqmvfmsde.supabase.co/rest/v1/${supaPath}`;

  try {
    const headers = {
      'apikey':        SERVICE_KEY,
      'Authorization': `Bearer ${SERVICE_KEY}`,
      'Content-Type':  'application/json',
    };
    if (req.headers['prefer']) headers['Prefer'] = req.headers['prefer'];

    const body = req.method !== 'GET' && req.method !== 'HEAD' && req.body
      ? JSON.stringify(req.body)
      : undefined;

    const upstream = await fetch(url, { method: req.method, headers, body });
    const text = await upstream.text();
    res.status(upstream.status).setHeader('Content-Type', 'application/json').send(text);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
