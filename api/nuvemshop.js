// api/nuvemshop.js — Proxy server-side para a API da Nuvemshop
// Resolve CORS: o browser chama /api/nuvemshop (mesmo domínio Vercel),
// e este handler faz a chamada real para api.nuvemshop.com.br no servidor.

export default async function handler(req, res) {
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  const { storeId, from, to, perPage, page } = req.query;
  const nsToken = req.headers['x-ns-token'];

  if (!storeId || !nsToken) {
    return res.status(400).json({ error: 'Missing storeId or x-ns-token header' });
  }

  const url = [
    `https://api.nuvemshop.com.br/v1/${storeId}/orders`,
    `?payment_status=paid`,
    `&created_at_min=${from}`,
    `&created_at_max=${to}T23:59:59`,
    `&per_page=${perPage || 200}`,
    `&page=${page || 1}`
  ].join('');

  try {
    const upstream = await fetch(url, {
      headers: {
        'Authentication': `bearer ${nsToken}`,
        'User-Agent': 'Metricana/1.0 (trafego@duacriativa.com.br)'
      }
    });

    if (!upstream.ok) {
      const txt = await upstream.text();
      return res.status(upstream.status).json({ error: txt.slice(0, 300) });
    }

    const data = await upstream.json();
    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
