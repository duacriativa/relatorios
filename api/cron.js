// api/cron.js — Metricana Weekly Report Automation
// Roda toda segunda-feira às 09:00 (BRT = UTC-3 → 12:00 UTC)
// Vercel Cron: "0 12 * * 1"

export const config = { maxDuration: 300 };

const SUPABASE_URL  = 'https://ugvmbtufwtpfqmvfmsde.supabase.co';
const SUPABASE_ANON = process.env.SUPABASE_ANON_KEY;

// ── Helpers ──────────────────────────────────────────────────────────────────

function fmtBRL(v) {
  return 'R$ ' + parseFloat(v || 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}
function fmtN(v) {
  return parseInt(v || 0).toLocaleString('pt-BR');
}
function fmt2(d) {
  if (!d) return '';
  const [y, m, dd] = d.split('-');
  return `${dd}/${m}/${y}`;
}

/** Retorna { from: 'YYYY-MM-DD', to: 'YYYY-MM-DD' } da semana anterior (seg-dom)
 *  Cron roda toda segunda. Semana anterior = seg passada (hoje-6) a dom passado (hoje-1)
 */
function getLastWeek() {
  const now = new Date();
  const toISO = d => d.toISOString().slice(0, 10);
  const sun = new Date(now); sun.setUTCDate(now.getUTCDate() - 1); // domingo passado
  const mon = new Date(now); mon.setUTCDate(now.getUTCDate() - 6); // segunda passada
  return { from: toISO(mon), to: toISO(sun) };
}

// ── Supabase ──────────────────────────────────────────────────────────────────

async function sbGet(path) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
    headers: {
      'apikey': SUPABASE_ANON,
      'Authorization': `Bearer ${SUPABASE_ANON}`,
    }
  });
  if (!res.ok) throw new Error(`Supabase ${res.status}: ${await res.text()}`);
  return res.json();
}

// ── Meta Ads API ──────────────────────────────────────────────────────────────

async function fetchMetaInsights(accountId, token, from, to) {
  const fields = [
    'spend', 'reach', 'impressions', 'clicks', 'ctr', 'cpm',
    'actions', 'action_values', 'cost_per_action_type',
    'inline_link_clicks', 'website_ctr'
  ].join(',');

  const url = `https://graph.facebook.com/v19.0/${accountId}/insights` +
    `?fields=${fields}` +
    `&time_range={"since":"${from}","until":"${to}"}` +
    `&level=account` +
    `&access_token=${token}`;

  const res  = await fetch(url);
  const json = await res.json();
  if (json.error) throw new Error(`Meta API: ${json.error.message}`);
  return json.data?.[0] || null;
}

/** Extrai valor de um tipo de ação (ex: 'omni_initiated_checkout', 'offsite_conversion.fb_pixel_purchase') */
function getAction(actions, type) {
  if (!Array.isArray(actions)) return 0;
  const a = actions.find(x => x.action_type === type);
  return parseFloat(a?.value || 0);
}
function getActionValue(action_values, type) {
  if (!Array.isArray(action_values)) return 0;
  const a = action_values.find(x => x.action_type === type);
  return parseFloat(a?.value || 0);
}

/** Monta objeto de dados normalizado igual ao fetchedData do front */
function parseMetaData(raw, from, to) {
  if (!raw) return null;
  const spend  = parseFloat(raw.spend || 0);
  const impr   = parseInt(raw.impressions || 0);
  const reach  = parseInt(raw.reach || 0);
  const clicks = parseInt(raw.clicks || 0);
  const ctr    = parseFloat(raw.ctr || 0);
  const cpm    = parseFloat(raw.cpm || 0);
  const lc     = parseInt(raw.inline_link_clicks || 0);

  const actions       = raw.actions || [];
  const action_values = raw.action_values || [];

  // Conversões e receita (pixel)
  const convQty = getAction(actions, 'offsite_conversion.fb_pixel_purchase')
               || getAction(actions, 'omni_purchase');
  const revVal  = getActionValue(action_values, 'offsite_conversion.fb_pixel_purchase')
               || getActionValue(action_values, 'omni_purchase');
  const roi     = spend > 0 && revVal > 0 ? (revVal / spend).toFixed(2) : '—';
  const cpa     = convQty > 0 ? (spend / convQty).toFixed(2) : '—';

  // Mensagens
  const msgs    = getAction(actions, 'onsite_conversion.messaging_conversation_started_7d')
               + getAction(actions, 'onsite_conversion.total_messaging_connection');
  const msgsWPP = getAction(actions, 'onsite_conversion.messaging_conversation_started_7d');
  const igDirect= getAction(actions, 'onsite_conversion.total_messaging_connection');

  // Visitas ao perfil / seguidores
  const profileVisits = getAction(actions, 'page_engagement')
                     || getAction(actions, 'profile_visit');
  const followers     = getAction(actions, 'like') + getAction(actions, 'follow');

  // Leads
  const leads = getAction(actions, 'lead');

  return {
    from, to, spend, impr, reach, clicks, ctr, cpm,
    linkClicks: lc,
    convQty, revVal, roi, cpa,
    msgs: Math.round(msgs), msgsWPP: Math.round(msgsWPP), igDirect: Math.round(igDirect),
    profileVisits: Math.round(profileVisits),
    followers: Math.round(followers),
    leads: Math.round(leads),
    // Nuvemshop fields (preenchidos depois se houver)
    nsSource: false, nsRevenue: 0, nsOrders: 0, nsProductsSold: 0,
    nsNewCustomers: 0, nsReturningCustomers: 0, nsCouponsUsed: 0, nsTrafficSources: []
  };
}

// ── Nuvemshop ─────────────────────────────────────────────────────────────────

async function fetchNuvemshop(storeId, nsToken, from, to) {
  let totalRevenue = 0, totalOrders = 0, totalProductsSold = 0, couponsUsed = 0;
  const seenCustomers = new Set(), newCustomerIds = new Set(), returningCustomerIds = new Set();
  const trafficMap = {};
  let page = 1;
  const fromDate = new Date(from + 'T00:00:00');
  const toDate   = new Date(to   + 'T23:59:59');

  while (true) {
    const url = `https://api.tiendanube.com/v1/${storeId}/orders` +
      `?created_at_min=${from}T00:00:00` +
      `&created_at_max=${to}T23:59:59` +
      `&per_page=200&page=${page}` +
      `&status=paid,closed`;

    const res = await fetch(url, {
      headers: {
        'Authentication': `bearer ${nsToken}`,
        'User-Agent': 'Metricana (suporte@duacriativa.com)',
        'Content-Type': 'application/json'
      }
    });
    if (!res.ok) {
      const txt = await res.text();
      throw new Error(`Nuvemshop HTTP ${res.status}: ${txt.slice(0, 200)}`);
    }
    const orders = await res.json();
    if (!Array.isArray(orders) || orders.length === 0) break;

    for (const o of orders) {
      const orderTotal = parseFloat(o.total || 0);
      totalRevenue += orderTotal;
      totalOrders++;
      if (Array.isArray(o.products)) {
        for (const p of o.products) totalProductsSold += parseInt(p.quantity || 1);
      }
      if ((Array.isArray(o.coupon) && o.coupon.length > 0) ||
          (o.coupon && typeof o.coupon === 'object' && o.coupon.code)) {
        couponsUsed++;
      }
      const custId = o.customer?.id;
      if (custId && !seenCustomers.has(custId)) {
        seenCustomers.add(custId);
        const custCreated = o.customer.created_at ? new Date(o.customer.created_at) : null;
        if (custCreated && custCreated >= fromDate && custCreated <= toDate) {
          newCustomerIds.add(custId);
        } else {
          returningCustomerIds.add(custId);
        }
      }
      const mktType  = o.marketing?.type || o.utm_source || 'Direto/Desconhecido';
      const mktLabel = ({ organic:'Orgânico', direct:'Direto', email:'E-mail',
        referral:'Referência', social:'Social', paid_search:'Busca Paga',
        display:'Display', affiliate:'Afiliado', sms:'SMS' })[mktType] || mktType;
      if (!trafficMap[mktLabel]) trafficMap[mktLabel] = { orders: 0, revenue: 0 };
      trafficMap[mktLabel].orders++;
      trafficMap[mktLabel].revenue += orderTotal;
    }
    if (orders.length < 200) break;
    page++;
    await new Promise(r => setTimeout(r, 600));
  }

  return {
    revenue: totalRevenue, orders: totalOrders, productsSold: totalProductsSold,
    couponsUsed, newCustomers: newCustomerIds.size,
    returningCustomers: returningCustomerIds.size,
    trafficSources: Object.entries(trafficMap)
      .map(([source, d]) => ({ source, ...d }))
      .sort((a, b) => b.orders - a.orders)
  };
}

// ── OpenAI — Análise IA ───────────────────────────────────────────────────────

async function genAnalysis(clientName, d, template, openaiKey) {
  if (!openaiKey) return null;

  const isMsgs = template === 'mensagens';
  const spend  = parseFloat(d.spend || 0);
  const msgs   = d.msgs || 0;
  const lc     = d.linkClicks || d.clicks || 0;

  const dadosAI = isMsgs
    ? `Alcance: ${fmtN(d.reach || d.impr)}
${lc > 0 ? 'Cliques no link' : 'Cliques'}: ${fmtN(lc)}
${lc > 0 ? 'CPS: R$ ' + (spend / lc).toFixed(2) : ''}
CTR: ${parseFloat(d.ctr).toFixed(2)}%
${d.profileVisits > 0 ? 'Visitas ao perfil IG: ' + fmtN(d.profileVisits) : ''}
${d.profileVisits > 0 ? 'Custo por visita: R$ ' + (spend / d.profileVisits).toFixed(2) : ''}
${d.followers > 0 ? 'Seguidores conquistados: ' + fmtN(d.followers) : ''}
${msgs > 0 ? 'Mensagens iniciadas: ' + fmtN(msgs) : ''}
${msgs > 0 ? 'Custo por mensagem: R$ ' + (spend / msgs).toFixed(2) : ''}
Investido: ${fmtBRL(spend)}`
    : `[META ADS]
Investido: ${fmtBRL(spend)}
${d.revVal > 0 ? 'Receita anúncios: R$ ' + parseFloat(d.revVal).toFixed(2) : ''}
ROI: ${d.roi !== '—' ? d.roi + 'x' : '—'}
Conversões: ${d.convQty}
${d.convQty > 0 && d.revVal > 0 ? 'Ticket médio (Meta): R$ ' + (d.revVal / d.convQty).toFixed(2) : ''}
${d.cpa !== '—' ? 'CPA: R$ ' + parseFloat(d.cpa).toFixed(2) : ''}
Cliques: ${d.clicks} | CTR: ${parseFloat(d.ctr).toFixed(2)}% | CPM: R$ ${parseFloat(d.cpm).toFixed(2)}
${msgs > 0 ? 'Mensagens: ' + msgs : ''}
${d.nsSource ? `
[NUVEMSHOP]
Pedidos totais: ${d.nsOrders}
Receita total: R$ ${parseFloat(d.nsRevenue).toFixed(2)}
${d.nsOrders > 0 ? 'Ticket médio real: R$ ' + (d.nsRevenue / d.nsOrders).toFixed(2) : ''}
Clientes novos: ${d.nsNewCustomers} | Recorrentes: ${d.nsReturningCustomers}
${d.nsProductsSold > 0 ? 'Produtos vendidos: ' + d.nsProductsSold : ''}` : ''}`;

  const regrasExtra = isMsgs
    ? '- Cliente de tráfego para perfil e mensagens. Fale sobre alcance, cliques, mensagens e custo por mensagem.'
    : d.nsSource
      ? '- Cliente de e-commerce. Você tem dados do Meta Ads e da Nuvemshop. Destaque a diferença entre conversões do pixel e pedidos reais da loja.'
      : '- Cliente de e-commerce. Foque em ROI, conversões e custo por venda.';

  const prompt = `Você é gestor de tráfego sênior. Escreva os "Próximos Passos" para o cliente "${clientName}" com base nos dados da semana.

FORMATO OBRIGATÓRIO — siga exatamente esta estrutura, sem markdown, sem asteriscos:

📌 Próximos Passos

[🟢 🟡 ou 🔴 conforme performance] [Uma frase avaliando o desempenho geral com dados reais]

[Título da 1ª recomendação]

[1-2 linhas com análise + ação prática]

[Título da 2ª recomendação]

[1-2 linhas com análise + sugestão]
${d.nsSource ? `
Diferença site x Meta

[Comparar pedidos Nuvemshop vs conversões Meta com os números reais]` : ''}
Próxima semana

[1 ação concreta e específica]

REGRAS:
- Use os dados reais nas frases
- Seja direto e acionável
- Máximo 200 palavras
- SEM asteriscos, SEM markdown
${regrasExtra}

Dados:
Período: ${fmt2(d.from)} a ${fmt2(d.to)}
${dadosAI}

Escreva APENAS o texto final, sem introdução.`;

  const res = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${openaiKey}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model: 'gpt-4o-mini', messages: [{ role: 'user', content: prompt }], max_tokens: 350, temperature: 0.75 })
  });
  const json = await res.json();
  if (json.error) throw new Error(`OpenAI: ${json.error.message}`);
  return json.choices?.[0]?.message?.content || null;
}

// ── Montar texto do relatório ─────────────────────────────────────────────────

function buildComment(client, d, template, aiText, period) {
  const { from, to } = period;
  const spend  = parseFloat(d.spend || 0);
  const msgs   = d.msgs || 0;
  const lc     = d.linkClicks || d.clicks || 0;
  const custoMsg = msgs > 0 ? fmtBRL(spend / msgs) : '—';

  const header = [
    `Oi pessoal! Tudo bem?`,
    `Vou enviar para vocês o resumo da nossa semana anterior de campanhas 👇`,
    ``,
    `📊 *Relatório Semanal Meta Ads — ${client.name}*`,
    `📅 ${fmt2(from)} – ${fmt2(to)}`,
    ``
  ];

  let body = [];

  if (template === 'mensagens') {
    body = [
      `📱 *Tráfego p/ Perfil IG*`,
      `👁️ Alcance: ${fmtN(d.reach || d.impr)}`,
      lc > 0 ? `👆 Cliques no link: ${fmtN(lc)}` : `👆 Cliques: ${fmtN(d.clicks)}`,
      lc > 0 ? `🖱️ CPS: ${fmtBRL(spend / lc)}` : null,
      `🎯 CTR: ${parseFloat(d.ctr).toFixed(2)}%`,
      d.profileVisits > 0 ? `👤 Visitas ao Perfil: ${fmtN(d.profileVisits)}` : null,
      d.profileVisits > 0 ? `💲 Custo/Visita: ${fmtBRL(spend / d.profileVisits)}` : null,
      d.followers > 0 ? `👤 Seguidores: ${fmtN(d.followers)}` : null,
      msgs > 0 ? `💬 Mensagens (Total): ${fmtN(msgs)}` : null,
      msgs > 0 ? `💲 Custo/Msg: ${custoMsg}` : null,
      ``,
      `💰 *Investimento: ${fmtBRL(spend)}*`,
    ];
  } else if (template === 'ecommerce') {
    const nsTicket = d.nsSource && d.nsOrders > 0 ? fmtBRL(d.nsRevenue / d.nsOrders) : null;
    body = [
      `📣 *Meta Ads*`,
      `💰 Investido: ${fmtBRL(spend)}`,
      d.revVal > 0 ? `💵 Receita (anúncios): ${fmtBRL(d.revVal)}` : null,
      d.roi !== '—' ? `📈 ROI: ${d.roi}x` : null,
      `🛒 Conversões: ${d.convQty}`,
      d.cpa !== '—' ? `💲 CPA: R$ ${parseFloat(d.cpa).toFixed(2)}` : null,
      d.convQty > 0 && d.revVal > 0 ? `🎯 Ticket Médio: ${fmtBRL(d.revVal / d.convQty)}` : null,
      `👆 Cliques: ${fmtN(d.clicks)}`,
      `👁️ Impressões: ${fmtN(d.impr)}`,
      `🎯 CTR: ${parseFloat(d.ctr).toFixed(2)}%`,
      `💡 CPM: R$ ${parseFloat(d.cpm).toFixed(2)}`,
      msgs > 0 ? `💬 Mensagens: ${fmtN(msgs)}` : null,
      d.nsSource ? `` : null,
      d.nsSource ? `🛍️ *Nuvemshop — Dados da Loja*` : null,
      d.nsSource ? `📦 Pedidos: ${d.nsOrders}` : null,
      d.nsSource ? `💵 Receita total: ${fmtBRL(d.nsRevenue)}` : null,
      nsTicket ? `🎯 Ticket Médio (NS): ${nsTicket}` : null,
      d.nsSource && d.nsProductsSold > 0 ? `📦 Produtos vendidos: ${fmtN(d.nsProductsSold)}` : null,
    ];
  } else {
    // completo — Meta Ads + Nuvemshop + Mensagens/Tráfego
    const nsRev    = d.nsSource && d.nsRevenue > 0 ? d.nsRevenue : d.revVal;
    const nsOrd    = d.nsSource && d.nsOrders  > 0 ? d.nsOrders  : d.convQty;
    const nsROI    = spend > 0 && nsRev > 0 ? (nsRev / spend).toFixed(2) : d.roi;
    const nsCPA    = nsOrd > 0 ? fmtBRL(spend / nsOrd) : (d.cpa !== '—' ? fmtBRL(parseFloat(d.cpa)) : '—');
    const nsTicket = nsOrd > 0 && nsRev > 0 ? fmtBRL(nsRev / nsOrd) : null;

    body = [
      `📣 *Meta Ads — Conversão*`,
      `💰 Investido: ${fmtBRL(spend)}`,
      nsRev > 0 ? `💵 Receita (anúncios): ${fmtBRL(nsRev)}` : null,
      nsROI !== '—' ? `📈 ROI: ${nsROI}x` : null,
      `🛒 Conversões: ${nsOrd}`,
      nsCPA !== '—' ? `💲 CPA: ${nsCPA}` : null,
      nsTicket ? `🎯 Ticket Médio: ${nsTicket}` : null,
      `👆 Cliques: ${fmtN(d.clicks)}`,
      `👁️ Impressões: ${fmtN(d.impr)}`,
      `🎯 CTR: ${parseFloat(d.ctr).toFixed(2)}%`,
      `💡 CPM: R$ ${parseFloat(d.cpm).toFixed(2)}`,
      d.nsSource ? `` : null,
      d.nsSource ? `🛍️ *Nuvemshop — Dados da Loja*` : null,
      d.nsSource ? `📦 Pedidos: ${d.nsOrders}` : null,
      d.nsSource ? `💵 Receita total: ${fmtBRL(d.nsRevenue)}` : null,
      d.nsSource && d.nsOrders > 0 ? `🎯 Ticket Médio (NS): ${fmtBRL(d.nsRevenue / d.nsOrders)}` : null,
      d.nsSource && d.nsProductsSold > 0 ? `📦 Produtos vendidos: ${fmtN(d.nsProductsSold)}` : null,
      ``,
      msgs > 0 ? `📱 *Tráfego / Mensagens*` : null,
      msgs > 0 ? `👁️ Alcance: ${fmtN(d.reach || d.impr)}` : null,
      lc > 0 ? `👆 Cliques no link: ${fmtN(lc)}` : null,
      lc > 0 ? `🖱️ CPS: ${fmtBRL(spend / lc)}` : null,
      msgs > 0 ? `🎯 CTR: ${parseFloat(d.ctr).toFixed(2)}%` : null,
      d.profileVisits > 0 ? `👤 Visitas ao Perfil: ${fmtN(d.profileVisits)}` : null,
      d.profileVisits > 0 ? `💲 Custo/Visita: ${fmtBRL(spend / d.profileVisits)}` : null,
      msgs > 0 ? `💬 Mensagens: ${fmtN(msgs)}` : null,
      msgs > 0 ? `💲 Custo/Msg: ${custoMsg}` : null,
    ];
  }

  const lines = [...header, ...body.filter(l => l !== null)];

  // Análise IA (se disponível)
  if (aiText) {
    lines.push('');
    lines.push('📌 *Principais destaques da semana:*');
    lines.push('');
    lines.push(aiText);
  }

  return lines.join('\n');
}

// ── ClickUp ───────────────────────────────────────────────────────────────────

async function sendToClickUp(cuToken, listId, clientName, comment) {
  const headers = { 'Authorization': cuToken, 'Content-Type': 'application/json' };

  // Buscar todas as tasks da lista
  const listRes = await fetch(`https://api.clickup.com/api/v2/list/${listId}/task?include_closed=true`, { headers });
  const listJson = await listRes.json();
  const tasks = listJson.tasks || [];

  // Encontrar task pelo nome "Relatório Semanal - [cliente]"
  const clientLower = clientName.toLowerCase().trim();
  const task = tasks.find(t => t.name.toLowerCase().includes(clientLower));

  if (task) {
    // Adiciona comentário na task existente
    await fetch(`https://api.clickup.com/api/v2/task/${task.id}/comment`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ comment_text: comment, notify_all: true })
    });
    return { action: 'comment', taskName: task.name };
  } else {
    // Cria nova task
    const createRes = await fetch(`https://api.clickup.com/api/v2/list/${listId}/task`, {
      method: 'POST',
      headers,
      body: JSON.stringify({ name: `Relatório Semanal - ${clientName}`, description: comment })
    });
    const created = await createRes.json();
    return { action: 'created', taskName: created.name };
  }
}

// ── Handler principal ─────────────────────────────────────────────────────────

export default async function handler(req, res) {
  // Segurança: só Vercel Cron ou requisição com secret header
  const authHeader = req.headers['authorization'];
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && authHeader !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const from = (req.query && req.query.from) || getLastWeek().from;
  const to   = (req.query && req.query.to)   || getLastWeek().to;
  console.log(`[cron] Período: ${from} → ${to}`);

  // Carregar config do Supabase
  const [configRows, clients] = await Promise.all([
    sbGet('config?id=eq.1&select=clickup_token,clickup_list_id,openai_key,meta_token'),
    sbGet('clientes?select=*&name=neq.Dua%20Criativa')
  ]);

  const cfg = configRows[0];
  if (!cfg?.clickup_token || !cfg?.clickup_list_id) {
    return res.status(500).json({ error: 'Config incompleta no Supabase' });
  }

  const results = [];

  for (const client of clients) {
    const clientResult = { name: client.name, status: 'ok', error: null };
    try {
      const token     = client.token || cfg.meta_token;
      const template  = client.report_template || 'mensagens';
      const hasNS     = !!(client.nuvemshop_store_id && client.nuvemshop_token);

      console.log(`[cron] Processando ${client.name} (template: ${template})`);

      // 1. Puxar Meta Ads
      const raw = await fetchMetaInsights(client.account_id, token, from, to);
      const d   = parseMetaData(raw, from, to);
      if (!d) throw new Error('Sem dados do Meta para o período');

      // 2. Puxar Nuvemshop (se tiver)
      if (hasNS && template !== 'mensagens') {
        try {
          const ns = await fetchNuvemshop(client.nuvemshop_store_id, client.nuvemshop_token, from, to);
          d.nsSource            = true;
          d.nsRevenue           = ns.revenue;
          d.nsOrders            = ns.orders;
          d.nsProductsSold      = ns.productsSold;
          d.nsNewCustomers      = ns.newCustomers;
          d.nsReturningCustomers = ns.returningCustomers;
          d.nsCouponsUsed       = ns.couponsUsed;
          d.nsTrafficSources    = ns.trafficSources;
        } catch(e) {
          console.warn(`[cron] Nuvemshop ${client.name}: ${e.message}`);
          clientResult.nsError = e.message;
        }
      }

      // 3. Gerar análise IA (só para clientes que usam)
      const useAI = ['mensagens', 'completo'].includes(template);
      let aiText = null;
      if (useAI && cfg.openai_key) {
        try {
          aiText = await genAnalysis(client.name, d, template, cfg.openai_key);
        } catch(e) {
          console.warn(`[cron] OpenAI ${client.name}: ${e.message}`);
        }
      }

      // 4. Montar comentário
      const comment = buildComment(client, d, template, aiText, { from, to });

      // 5. Enviar ao ClickUp
      const cuResult = await sendToClickUp(cfg.clickup_token, cfg.clickup_list_id, client.name, comment);
      clientResult.clickup = cuResult;

      console.log(`[cron] ✓ ${client.name} → ${cuResult.action} em "${cuResult.taskName}"`);

    } catch(e) {
      clientResult.status = 'error';
      clientResult.error  = e.message;
      console.error(`[cron] ✗ ${client.name}: ${e.message}`);
    }

    results.push(clientResult);

    // Rate limit: 1 cliente por vez com intervalo
    await new Promise(r => setTimeout(r, 1000));
  }

  const ok     = results.filter(r => r.status === 'ok').length;
  const errors = results.filter(r => r.status === 'error').length;
  console.log(`[cron] Concluído: ${ok} ok, ${errors} erros`);

  return res.status(200).json({ period: { from, to }, ok, errors, results });
}
