// /api/cron.js — Metricana Automação Semanal
// Chamado pelo cron-job.org toda segunda às 09h (horário de Brasília)
// Usa o proxy /api/supabase interno — sem env vars extras necessárias

// ─── Datas: últimos 7 dias (D-7 até D-1) ───
function getPeriod() {
  // Forçar fuso horário de Brasília (UTC-3)
  const now = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/Fortaleza' }));
  const to = new Date(now);
  to.setDate(to.getDate() - 1); // ontem
  const from = new Date(to);
  from.setDate(to.getDate() - 6); // 7 dias atrás
  const pad = n => String(n).padStart(2, '0');
  const fmt  = d => `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
  const fmt2 = d => `${pad(d.getDate())}/${pad(d.getMonth()+1)}/${d.getFullYear()}`;
  return { since: fmt(from), until: fmt(to), labelFrom: fmt2(from), labelTo: fmt2(to) };
}

// ─── Chama o proxy interno do Supabase ───
async function sbGet(baseUrl, path) {
  const res = await fetch(`${baseUrl}/api/supabase?p=${encodeURIComponent(path)}`);
  if (!res.ok) throw new Error(`Supabase proxy ${res.status}: ${await res.text()}`);
  return res.json();
}

// ─── Formata BRL ───
function fmtBRL(v) {
  return 'R$ ' + parseFloat(v).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

// ─── Meta Ads insights ───
async function fetchMetaInsights(accountId, token, since, until) {
  const fields = 'spend,clicks,inline_link_clicks,impressions,reach,ctr,cpm,actions,action_values';
  const timeRange = `{"since":"${since}","until":"${until}"}`;
  const url = `https://graph.facebook.com/v18.0/${accountId}/insights?fields=${fields}&time_range=${timeRange}&level=account&access_token=${token}`;

  const ctrl  = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 20000);
  try {
    const res  = await fetch(url, { signal: ctrl.signal });
    clearTimeout(timer);
    const data = await res.json();
    if (data.error) throw new Error(`Meta (${accountId}): ${data.error.message}`);
    const ins = data.data?.[0];
    if (!ins) return null;

    const spend      = parseFloat(ins.spend || 0);
    const clicks     = parseInt(ins.clicks || 0);
    const linkClicks = parseInt(ins.inline_link_clicks || 0);
    const impr       = parseInt(ins.impressions || 0);
    const reach      = parseInt(ins.reach || 0);
    const ctr        = parseFloat(ins.ctr || 0);
    const cpm        = parseFloat(ins.cpm || 0);
    const actions    = ins.actions || [];
    const actVals    = ins.action_values || [];
    const getA  = type => parseFloat(actions.find(a => a.action_type === type)?.value || 0);
    const getAV = type => parseFloat(actVals.find(a => a.action_type === type)?.value || 0);

    const convQty  = getA('purchase');
    const revVal   = getAV('purchase');
    const msgs     = getA('onsite_conversion.messaging_conversation_started_7d');
    const msgsWPP  = getA('onsite_conversion.messaging_whatsapp_conversation_started_7d');
    const igDirect = getA('onsite_conversion.messaging_instagram_conversation_started_7d');
    const profileVisits = getA('instagram_profile_visit') || getA('view_content');
    const followers     = getA('instagram_follow') || getA('page_fan');
    const leads         = getA('lead');
    const lc  = linkClicks || clicks;
    const roi = spend > 0 && revVal > 0 ? (revVal / spend).toFixed(2) : null;
    const cpa = convQty > 0 ? (spend / convQty).toFixed(2) : null;

    return { spend, clicks, linkClicks, lc, impr, reach, ctr, cpm, convQty, revVal, roi, cpa, msgs, msgsWPP, igDirect, profileVisits, followers, leads };
  } catch(e) {
    clearTimeout(timer);
    throw e;
  }
}

// ─── Monta texto do relatório ───
function buildReportText(clientName, d, labelFrom, labelTo) {
  const isEcommerce = d.convQty > 0 || d.revVal > 0;
  const hasMsgs     = d.msgs > 0 || d.msgsWPP > 0 || d.igDirect > 0;
  const lc = d.lc;

  const lines = [
    `📊 *Relatório Semanal Meta Ads — ${clientName}*`,
    `📅 ${labelFrom} – ${labelTo}`,
    ``,
  ];

  if (isEcommerce) {
    lines.push(`🛒 *E-commerce*`);
    lines.push(`💰 Investido: ${fmtBRL(d.spend)}`);
    if (d.revVal > 0) lines.push(`💵 Receita: ${fmtBRL(d.revVal)}`);
    if (d.roi)        lines.push(`📈 ROI: ${d.roi}x`);
    if (d.convQty > 0) lines.push(`🛒 Conversões: ${d.convQty}`);
    if (d.cpa)        lines.push(`💲 CPA: R$ ${d.cpa}`);
    lines.push(``);
  }

  if (hasMsgs || !isEcommerce) {
    lines.push(`📱 *Tráfego p/ Perfil IG*`);
    lines.push(`👁️ Alcance: ${(d.reach || d.impr).toLocaleString('pt-BR')}`);
    if (lc > 0) {
      lines.push(`👆 Cliques no link: ${lc.toLocaleString('pt-BR')}`);
      lines.push(`🖱️ CPS: ${fmtBRL(d.spend / lc)}`);
    }
    lines.push(`🎯 CTR: ${parseFloat(d.ctr).toFixed(2)}%`);
    if (d.profileVisits > 0) {
      lines.push(`👤 Visitas ao Perfil: ${d.profileVisits.toLocaleString('pt-BR')}`);
      lines.push(`💲 Custo/Visita: ${fmtBRL(d.spend / d.profileVisits)}`);
    }
    if (d.followers > 0)  lines.push(`➕ Seguidores: ${d.followers.toLocaleString('pt-BR')}`);
    if (d.msgs > 0)       lines.push(`💬 Mensagens (Total): ${d.msgs.toLocaleString('pt-BR')}`);
    if (d.msgsWPP > 0) {
      lines.push(`📨 Msgs WhatsApp: ${d.msgsWPP.toLocaleString('pt-BR')}`);
      lines.push(`🟢 Custo/Msg WPP: ${fmtBRL(d.spend / d.msgsWPP)}`);
    }
    if (d.igDirect > 0) {
      lines.push(`📩 Msgs Direct (IG): ${d.igDirect.toLocaleString('pt-BR')}`);
      lines.push(`💌 Custo/Msg Direct: ${fmtBRL(d.spend / d.igDirect)}`);
    }
    if (d.msgs > 0) lines.push(`💲 Custo/Msg: ${fmtBRL(d.spend / d.msgs)}`);
    lines.push(``);
  }

  lines.push(`💰 *Investimento: ${fmtBRL(d.spend)}*`);
  return lines.join('\n');
}

// ─── Gemini — Próximos Passos ───
async function genAI(clientName, d, geminiKey, labelFrom, labelTo) {
  if (!geminiKey) return null;
  const isEcommerce = d.convQty > 0 || d.revVal > 0;
  const lc = d.lc;
  const dadosStr = isEcommerce
    ? `Investido: R$ ${d.spend.toFixed(2)}\nReceita: ${d.revVal > 0 ? 'R$ '+d.revVal.toFixed(2) : '—'}\nROI: ${d.roi ? d.roi+'x' : '—'}\nConversões: ${d.convQty}\nCPA: ${d.cpa ? 'R$ '+d.cpa : '—'}\nCTR: ${d.ctr.toFixed(2)}%\nMensagens: ${d.msgs}`
    : `Investido: R$ ${d.spend.toFixed(2)}\nAlcance: ${d.reach || d.impr}\nCliques no link: ${lc}\nCPS: ${lc > 0 ? 'R$ '+(d.spend/lc).toFixed(2) : '—'}\nCTR: ${d.ctr.toFixed(2)}%\nMensagens iniciadas: ${d.msgs}\nCusto/Msg: ${d.msgs > 0 ? 'R$ '+(d.spend/d.msgs).toFixed(2) : '—'}\nVisitas ao perfil IG: ${d.profileVisits}\nSeguidores: ${d.followers}`;

  const prompt = `Você é gestor de tráfego sênior. Escreva os "Próximos Passos" para o cliente "${clientName}" com base nos dados da semana (${labelFrom} a ${labelTo}).

FORMATO — sem markdown, sem asteriscos:

📌 Próximos Passos

[🟢 🟡 ou 🔴] [Uma frase avaliando o desempenho com dados reais]

[Título da 1ª recomendação]

[1-2 linhas com análise + ação prática]

[Título da 2ª recomendação]

[1-2 linhas com análise + ação prática]

Próxima semana

[1 ação concreta]

REGRAS: Use os dados reais. Direto e acionável. Máximo 150 palavras. SEM asteriscos.

Dados:
${dadosStr}`;

  try {
    const ctrl  = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 18000);
    const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${geminiKey}`, {
      method: 'POST',
      signal: ctrl.signal,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{ parts: [{ text: prompt }] }],
        generationConfig: { maxOutputTokens: 220, temperature: 0.75 }
      })
    });
    clearTimeout(timer);
    const data = await res.json();
    if (data.error) return null;
    return data.candidates?.[0]?.content?.parts?.[0]?.text || null;
  } catch(e) {
    return null; // IA falhou — não bloqueia o relatório
  }
}

// ─── ClickUp: encontra ou cria tarefa, adiciona comentário ───
async function sendToClickUp(clientName, reportText, aiText, clickupToken, listId, labelFrom, labelTo) {
  const headers = { 'Authorization': clickupToken, 'Content-Type': 'application/json' };

  // Busca tarefas abertas da lista
  const tasksRes  = await fetch(`https://api.clickup.com/api/v2/list/${listId}/task?include_closed=false&limit=100`, { headers });
  const tasksData = await tasksRes.json();
  const allTasks  = tasksData.tasks || [];

  const clientLow  = clientName.toLowerCase().trim();
  const clientWords = clientLow.split(/\s+/).filter(w => w.length > 2);
  const matchTask  = t => {
    const tl = t.name.toLowerCase();
    return tl.includes(clientLow) || clientWords.some(w => tl.includes(w));
  };

  // Comentário com header de automação
  const autoHeader  = `🤖 *Automação Metricana — semana ${labelFrom} a ${labelTo}*\n\n`;
  const finalComment = autoHeader + reportText;

  let taskId;
  const existing = allTasks.find(matchTask);

  if (existing) {
    taskId = existing.id;
    // Atualiza descrição com Próximos Passos da IA
    if (aiText) {
      await fetch(`https://api.clickup.com/api/v2/task/${taskId}`, {
        method: 'PUT', headers,
        body: JSON.stringify({ description: aiText })
      });
    }
  } else {
    // Cria nova tarefa
    const taskName = `Relatório Semanal — ${clientName} — ${labelFrom}`;
    const created = await fetch(`https://api.clickup.com/api/v2/list/${listId}/task`, {
      method: 'POST', headers,
      body: JSON.stringify({
        name: taskName,
        description: aiText || '',
        tags: ['relatório semanal'],
        priority: 2 // alta
      })
    });
    const createdData = await created.json();
    if (createdData.err) throw new Error(`ClickUp criar tarefa: ${createdData.err}`);
    taskId = createdData.id;
  }

  // Adiciona comentário com os dados do relatório
  const commentRes  = await fetch(`https://api.clickup.com/api/v2/task/${taskId}/comment`, {
    method: 'POST', headers,
    body: JSON.stringify({ comment_text: finalComment, notify_all: true })
  });
  const commentData = await commentRes.json();
  if (commentData.err) throw new Error(`ClickUp comentário: ${commentData.err}`);

  return { taskId, created: !existing, taskName: existing?.name || `Relatório Semanal — ${clientName}` };
}

// ════════ HANDLER PRINCIPAL ════════
export default async function handler(req, res) {
  if (req.method !== 'GET' && req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const startTime = Date.now();
  const { since, until, labelFrom, labelTo } = getPeriod();
  const results = [];

  // URL base para chamadas internas (proxy Supabase)
  const baseUrl = process.env.VERCEL_URL
    ? `https://${process.env.VERCEL_URL}`
    : 'https://relatorios-trafego.vercel.app';

  try {
    // 1. Carrega config via proxy interno
    const configRows = await sbGet(baseUrl, 'config?select=*&limit=1');
    if (!configRows?.length) {
      return res.status(500).json({ error: 'Config não encontrada no Supabase' });
    }
    const { clickup_token: clickupToken, clickup_list_id: listId, gemini_key: geminiKey, meta_token: globalToken } = configRows[0];

    if (!clickupToken || !listId) {
      return res.status(500).json({ error: 'ClickUp token ou list ID não configurados' });
    }

    // 2. Carrega clientes via proxy interno
    const clients = await sbGet(baseUrl, 'clientes?select=*&order=name.asc');
    if (!clients?.length) {
      return res.status(200).json({ message: 'Nenhum cliente cadastrado', period: `${labelFrom} – ${labelTo}` });
    }

    // 3. Processa cada cliente sequencialmente (evita rate limit da Meta)
    for (const client of clients) {
      const token     = (client.token && client.token !== 'DEMO') ? client.token : globalToken;
      const accountId = client.account_id;

      if (!token || !accountId) {
        results.push({ client: client.name, status: 'skipped', reason: 'sem token ou account_id' });
        continue;
      }

      try {
        // Busca dados do Meta com timeout
        const metaData = await fetchMetaInsights(accountId, token, since, until);

        if (!metaData || metaData.spend === 0) {
          results.push({ client: client.name, status: 'skipped', reason: 'sem investimento no período' });
          continue;
        }

        // Monta texto do relatório
        const reportText = buildReportText(client.name, metaData, labelFrom, labelTo);

        // Análise de IA desativada por enquanto
        const aiText = null;

        // Envia ao ClickUp
        const cuResult = await sendToClickUp(client.name, reportText, aiText, clickupToken, listId, labelFrom, labelTo);

        results.push({
          client: client.name,
          status: 'ok',
          spend: fmtBRL(metaData.spend),
          roi: metaData.roi ? `${metaData.roi}x` : '—',
          clickup: cuResult.created ? `tarefa criada: ${cuResult.taskName}` : `comentário em: ${cuResult.taskName}`,
          ai: aiText ? 'gerado' : 'sem IA',
        });

        // Pausa de 500ms entre clientes para não estressar a API Meta
        await new Promise(r => setTimeout(r, 500));

      } catch(e) {
        results.push({ client: client.name, status: 'error', error: e.message });
      }
    }

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    return res.status(200).json({
      success: true,
      period: `${labelFrom} – ${labelTo}`,
      elapsed: `${elapsed}s`,
      total: clients.length,
      processed: results.filter(r => r.status === 'ok').length,
      skipped:   results.filter(r => r.status === 'skipped').length,
      errors:    results.filter(r => r.status === 'error').length,
      results,
    });

  } catch(e) {
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    return res.status(500).json({ error: e.message, elapsed: `${elapsed}s`, results });
  }
}
