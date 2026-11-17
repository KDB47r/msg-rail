// 用户资料缓存：昵称 + 服务端签名的头像 URL。
// 所有页面共用；头像签名会过期，缓存时间按 avatar_expires_at 提前 30 秒截断。
(function () {
  'use strict';

  const cache = new Map();

  function apiBase() {
    return window.location.protocol + '//' + window.location.hostname + ':9101';
  }

  function authHeaders() {
    const h = { 'Content-Type': 'application/json' };
    const saved = window.Auth && Auth.get();
    if (saved && saved.token) h['Authorization'] = 'Bearer ' + saved.token;
    return h;
  }

  async function doFetch(userId) {
    const resp = await fetch(apiBase() + '/api/user/profile', {
      method: 'POST',
      headers: authHeaders(),
      body: JSON.stringify({ user_id: Number(userId) })
    });
    const body = await resp.json();
    if (!resp.ok || body.code !== 0) {
      const err = new Error(body.message || ('HTTP ' + resp.status));
      err.status = resp.status;
      throw err;
    }
    return body.data || {};
  }

  function expiryFor(profile, now) {
    const expires = Number(profile && profile.avatar_expires_at) || 0;
    if (expires > 0 && profile.avatar_url) {
      return Math.max(now + 5000, Math.min(now + 5 * 60 * 1000,
                                           expires * 1000 - 30000));
    }
    // 没有头像时资料变化频率低，缓存 30 秒；有 media_id 但签名 URL 为空
    // 说明 S3/媒体暂时不可用，短缓存，等恢复后尽快刷新。
    if (profile && profile.avatar_media_id && !profile.avatar_url) {
      return now + 5000;
    }
    return now + 30000;
  }

  function get(userId, force) {
    const id = Number(userId);
    if (!id || id < 0) return Promise.reject(new Error('invalid user id'));
    const now = Date.now();
    const hit = cache.get(id);
    if (!force && hit && hit.expiresAt > now) return hit.promise;

    const entry = { expiresAt: now + 30000, promise: null };
    entry.promise = doFetch(id).then(profile => {
      entry.expiresAt = expiryFor(profile, Date.now());
      return profile;
    }).catch(err => {
      cache.delete(id);
      throw err;
    });
    cache.set(id, entry);
    return entry.promise;
  }

  function prime(profile) {
    if (!profile || !profile.user_id) return;
    const id = Number(profile.user_id);
    const entry = {
      expiresAt: expiryFor(profile, Date.now()),
      promise: Promise.resolve(profile)
    };
    cache.set(id, entry);
  }

  function invalidate(userId) {
    cache.delete(Number(userId));
  }

  function displayName(profile, userId) {
    if (profile && profile.name) return profile.name;
    return 'User ' + userId;
  }

  function initials(profile, userId) {
    const name = (profile && profile.name) || '';
    const chars = Array.from(name.trim());
    if (chars.length) return chars[0];
    return String(userId || '?').slice(-2);
  }

  function applyAvatar(el, profile, userId) {
    if (!el) return;
    const url = profile && profile.avatar_url;
    if (url) {
      el.style.backgroundImage = 'url(' + JSON.stringify(url) + ')';
      el.textContent = '';
      el.classList.add('has-image');
    } else {
      el.style.backgroundImage = '';
      el.textContent = initials(profile, userId);
      el.classList.remove('has-image');
    }
  }

  // 先渲染占位文字，资料返回后自动替换头像和昵称。
  async function hydrate(el, userId, nameEl, fallbackName) {
    if (!el && !nameEl) return null;
    if (el) applyAvatar(el, null, userId);
    if (nameEl) nameEl.textContent = fallbackName || ('User ' + userId);
    try {
      const profile = await get(userId);
      if (el) applyAvatar(el, profile, userId);
      if (nameEl) nameEl.textContent = displayName(profile, userId);
      return profile;
    } catch (e) {
      return null;
    }
  }

  window.UserProfile = {
    get,
    prime,
    invalidate,
    displayName,
    applyAvatar,
    hydrate
  };
})();
