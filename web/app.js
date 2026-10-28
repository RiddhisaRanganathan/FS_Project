'use strict';
const $ = id => document.getElementById(id);
const money = value => new Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR', maximumFractionDigits: 0 }).format(value);
let attempt = JSON.parse(localStorage.getItem('orbit-attempt') || 'null');
let timer;
let busy = false;
let pollCount = 0;
let expiresAt = null;
let serverOffset = 0;
let lastStatus = null;
$('token').value = sessionStorage.getItem('orbit-token') || '';
function credentials() {
  sessionStorage.setItem('orbit-token', $('token').value.trim());
  return { 'Content-Type': 'application/json', Authorization: `Bearer ${$('token').value.trim()}` };
}
function message(state, text) {
  $('state').textContent = state;
  $('message').textContent = text;
}
function persist() {
  localStorage.setItem('orbit-attempt', JSON.stringify(attempt));
  $('quantity').value = attempt.quantity;
  $('quantity').disabled = true;
  $('plus').disabled = $('minus').disabled = true;
  $('submit').disabled = true;
  $('recover').hidden = false;
  $('total').textContent = money(attempt.quantity * 39900);
}
async function api(path, options = {}) {
  const response = await fetch(path, { ...options, headers: { ...credentials(), ...options.headers }, signal: AbortSignal.timeout(8000) });
  const data = await response.json();
  if (!response.ok) throw new Error(data.detail instanceof Array ? 'Check the entered quantity and access token.' : data.detail || 'Request failed');
  return data;
}
function render(data) {
  lastStatus = data.status;
  const descriptions = {
    QUEUED: 'Your request is safely recorded. Waiting for the full quantity to become available.',
    RESERVED: 'Your phones are reserved. Complete the simulated payment before the hold expires.',
    PAYMENT_PENDING: 'Payment is processing. We will confirm only after its result is recorded.',
    CONFIRMED: 'You’re all set. Payment is verified and your purchase is confirmed.',
    SOLD_OUT: 'The full requested quantity is no longer available. No payment was taken.',
    FAILED: data.reason || 'This checkout could not be completed.'
  };
  message(data.status.replaceAll('_', ' '), descriptions[data.status]);
  if (data.payment_state === 'REFUND_PENDING' || data.payment_state === 'REFUNDED') {
    $('message').textContent += data.payment_state === 'REFUNDED' ? ' The late simulated payment has been refunded.' : ' A late payment is awaiting a simulated refund.';
  }
  $('attempt-label').textContent = `Attempt ${data.id || attempt.id}`;
  $('payment-actions').hidden = data.status !== 'RESERVED';
  const terminal = ['CONFIRMED', 'FAILED', 'SOLD_OUT'].includes(data.status);
  $('new-attempt').hidden = !terminal;
  $('recover').hidden = terminal && !['PENDING', 'REFUND_PENDING'].includes(data.payment_state);
  expiresAt = !terminal && data.expires_at ? new Date(data.expires_at).getTime() : null;
  serverOffset = new Date(data.server_time).getTime() - Date.now();
  clearTimeout(timer);
  if (!terminal || ['PENDING', 'REFUND_PENDING'].includes(data.payment_state)) {
    const delay = Math.min(10000, 2000 + pollCount++ * 250) + Math.random() * 800;
    timer = setTimeout(poll, delay);
  }
}
async function poll() {
  if (!attempt?.id) return;
  try { render(await api(`/api/checkout-attempts/${attempt.id}`)); }
  catch (error) {
    message('STATUS UNAVAILABLE', `${error.message}. Your recorded attempt is preserved.`);
    $('recover').hidden = false;
    clearTimeout(timer);
    timer = setTimeout(poll, 7000 + Math.random() * 3000);
  }
}
async function submit() {
  if (busy) return;
  busy = true;
  persist();
  message('SUBMITTING', 'Recording your request. Keep this attempt if the connection is interrupted.');
  try {
    const result = await api('/api/checkout-attempts', { method: 'POST', headers: { 'Idempotency-Key': attempt.key },
      body: JSON.stringify({ saleId: 'festival-sale', sku: 'phone-001', quantity: attempt.quantity }) });
    attempt.id = result.attemptId;
    persist();
    await poll();
  } catch (error) {
    message('RETRY / RECOVER', `${error.message}. Recover with the same attempt; its outcome may already be recorded.`);
    $('recover').hidden = false;
  } finally { busy = false; }
}
$('checkout-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (!attempt) {
    const quantity = Number($('quantity').value);
    if (!Number.isInteger(quantity) || quantity < 1 || quantity > 500) return;
    attempt = { key: crypto.randomUUID(), quantity };
  }
  await submit();
});
$('recover').addEventListener('click', async () => {
  if (attempt?.id) return poll();
  if (!attempt || busy) return;
  try {
    const data = await api(`/api/checkout-attempts/by-key/${attempt.key}`);
    attempt.id = data.id;
    persist();
    render(data);
  } catch { await submit(); }
});
async function pay(outcome) {
  if (busy) return;
  busy = true;
  $('pay').disabled = $('decline').disabled = true;
  try {
    await api(`/api/checkout-attempts/${attempt.id}/payment`, { method: 'POST', body: JSON.stringify({ outcome, delaySeconds: 2 }) });
    message('PAYMENT PENDING', 'Waiting for the simulated provider result.');
    $('payment-actions').hidden = true;
    await poll();
  } catch (error) { message('PAYMENT STATUS UNCERTAIN', `${error.message}. Recover the attempt before retrying payment.`); }
  finally { busy = false; $('pay').disabled = $('decline').disabled = false; }
}
$('pay').addEventListener('click', () => pay('success'));
$('decline').addEventListener('click', () => pay('failure'));
$('new-attempt').addEventListener('click', () => {
  clearTimeout(timer);
  localStorage.removeItem('orbit-attempt');
  location.reload();
});
function quantityChange(delta = 0) {
  if (attempt) return;
  const quantity = Math.min(500, Math.max(1, Number($('quantity').value) + delta));
  $('quantity').value = quantity;
  $('total').textContent = money(quantity * 39900);
}
$('plus').addEventListener('click', () => quantityChange(1));
$('minus').addEventListener('click', () => quantityChange(-1));
$('quantity').addEventListener('change', () => quantityChange());
document.addEventListener('visibilitychange', () => { if (!document.hidden) poll(); });
setInterval(() => {
  $('countdown').textContent = expiresAt ? `Hold remaining: ${Math.max(0, Math.ceil((expiresAt - Date.now() - serverOffset) / 1000))}s · server verified` : '';
}, 1000);
if (attempt) {
  persist();
  message('RESTORING CHECKOUT', 'Enter your access token if needed, then recover the saved attempt.');
  if ($('token').value && attempt.id) poll();
}
