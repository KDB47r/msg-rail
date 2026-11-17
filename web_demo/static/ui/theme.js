/* MsgRail theme bootstrap: applies light/dark before first paint and binds toggles. */
(function () {
  'use strict';

  var KEY = 'msgrail-theme';
  var root = document.documentElement;
  var media = window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
  var stored = null;
  var current = 'light';

  try {
    stored = localStorage.getItem(KEY);
  } catch (e) {
    stored = null;
  }

  if (stored === 'light' || stored === 'dark') {
    current = stored;
  } else if (media && media.matches) {
    current = 'dark';
  }

  root.setAttribute('data-theme', current);
  root.classList.add('theme-ready');

  function apply(theme) {
    current = theme;
    root.setAttribute('data-theme', theme);
    var dark = theme === 'dark';

    document.querySelectorAll('[data-theme-toggle]').forEach(function (btn) {
      btn.setAttribute('aria-pressed', String(dark));
      btn.setAttribute('aria-label', dark ? '切换到浅色模式' : '切换到深色模式');
      btn.setAttribute('title', dark ? '切换到浅色模式' : '切换到深色模式');
    });
  }

  function persist(theme) {
    try {
      localStorage.setItem(KEY, theme);
    } catch (e) {
      /* Private mode or storage disabled: keep the in-memory value. */
    }
  }

  function bind() {
    apply(current);

    document.querySelectorAll('[data-theme-toggle]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var next = current === 'dark' ? 'light' : 'dark';
        persist(next);
        apply(next);
      });
    });

    if (media) {
      var onSystemChange = function (event) {
        var saved = null;
        try {
          saved = localStorage.getItem(KEY);
        } catch (e) {
          saved = null;
        }
        if (saved !== 'light' && saved !== 'dark') {
          apply(event.matches ? 'dark' : 'light');
        }
      };

      if (typeof media.addEventListener === 'function') {
        media.addEventListener('change', onSystemChange);
      } else if (typeof media.addListener === 'function') {
        media.addListener(onSystemChange);
      }
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', bind);
  } else {
    bind();
  }
})();
