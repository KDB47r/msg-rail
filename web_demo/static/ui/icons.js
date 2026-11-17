/* MsgRail icon sprite: injected once so every page can use <use href="#i-...">. */
(function () {
  'use strict';

  var SPRITE =
    '<svg width="0" height="0" style="position:absolute;overflow:hidden" aria-hidden="true" focusable="false">' +
    '<defs>' +
    '<symbol id="i-logo" viewBox="0 0 24 24">' +
    '<path d="M4 18V7.2L12 12l8-4.8V18" />' +
    '<circle cx="4" cy="7.2" r="1.5" fill="currentColor" stroke="none" />' +
    '<circle cx="20" cy="7.2" r="1.5" fill="currentColor" stroke="none" />' +
    '<circle cx="12" cy="12" r="1.5" fill="currentColor" stroke="none" />' +
    '</symbol>' +
    '<symbol id="i-arrow-left" viewBox="0 0 24 24"><path d="M19 12H5" /><path d="m11 6-6 6 6 6" /></symbol>' +
    '<symbol id="i-arrow-right" viewBox="0 0 24 24"><path d="M5 12h14" /><path d="m13 6 6 6-6 6" /></symbol>' +
    '<symbol id="i-chevron-right" viewBox="0 0 24 24"><path d="m9 6 6 6-6 6" /></symbol>' +
    '<symbol id="i-check" viewBox="0 0 24 24"><path d="m5 12.5 4.5 4.5L19 7" /></symbol>' +
    '<symbol id="i-close" viewBox="0 0 24 24"><path d="m6 6 12 12M18 6 6 18" /></symbol>' +
    '<symbol id="i-plus" viewBox="0 0 24 24"><path d="M12 5v14M5 12h14" /></symbol>' +
    '<symbol id="i-send" viewBox="0 0 24 24">' +
    '<path d="M4.5 11.5 20 4l-6.5 16-2.4-6.1L4.5 11.5Z" />' +
    '<path d="M11.1 13.9 20 4" />' +
    '</symbol>' +
    '<symbol id="i-sun" viewBox="0 0 24 24">' +
    '<circle cx="12" cy="12" r="4" />' +
    '<path d="M12 2.5v2M12 19.5v2M4.3 4.3l1.4 1.4M18.3 18.3l1.4 1.4M2.5 12h2M19.5 12h2M4.3 19.7l1.4-1.4M18.3 5.7l1.4-1.4" />' +
    '</symbol>' +
    '<symbol id="i-moon" viewBox="0 0 24 24"><path d="M20 14.2A8.5 8.5 0 0 1 9.8 4a8.5 8.5 0 1 0 10.2 10.2Z" /></symbol>' +
    '<symbol id="i-image" viewBox="0 0 24 24">' +
    '<rect x="3.5" y="4.5" width="17" height="15" rx="2.5" />' +
    '<circle cx="9" cy="10" r="1.5" />' +
    '<path d="m4.5 17 4.7-4.3a2 2 0 0 1 2.7 0l2.7 2.4 1.6-1.4a2 2 0 0 1 2.7.1l1.6 1.6" />' +
    '</symbol>' +
    '<symbol id="i-video" viewBox="0 0 24 24">' +
    '<rect x="3" y="5" width="13" height="14" rx="2.5" />' +
    '<path d="m16 10.5 5-2.7v8.4l-5-2.7" />' +
    '</symbol>' +
    '<symbol id="i-file" viewBox="0 0 24 24">' +
    '<path d="M6 3.5h7l5 5V20a1.5 1.5 0 0 1-1.5 1.5h-10A1.5 1.5 0 0 1 5 20V5a1.5 1.5 0 0 1 1-1.5Z" />' +
    '<path d="M13 3.8V9h5" /><path d="M9 13h6M9 16.5h4" />' +
    '</symbol>' +
    '<symbol id="i-search" viewBox="0 0 24 24"><circle cx="11" cy="11" r="6.5" /><path d="m16 16 4 4" /></symbol>' +
    '<symbol id="i-users" viewBox="0 0 24 24">' +
    '<circle cx="9" cy="8" r="3" /><path d="M3.5 19a5.5 5.5 0 0 1 11 0" />' +
    '<circle cx="17" cy="9" r="2.4" /><path d="M16 14.5a4.5 4.5 0 0 1 4.5 4.5" />' +
    '</symbol>' +
    '<symbol id="i-shield" viewBox="0 0 24 24">' +
    '<path d="M12 3.2 19 6v5.2c0 4.3-2.8 7.2-7 9.3-4.2-2.1-7-5-7-9.3V6l7-2.8Z" />' +
    '<path d="m9 12 2 2 4-4" />' +
    '</symbol>' +
    '<symbol id="i-terminal" viewBox="0 0 24 24">' +
    '<rect x="3.5" y="4.5" width="17" height="15" rx="2.5" />' +
    '<path d="m7.5 9 3 3-3 3" /><path d="M12.5 15h4" />' +
    '</symbol>' +
    '<symbol id="i-copy" viewBox="0 0 24 24">' +
    '<rect x="9" y="9" width="10" height="10" rx="2" />' +
    '<path d="M5 15V6a1 1 0 0 1 1-1h9" />' +
    '</symbol>' +
    '<symbol id="i-play" viewBox="0 0 24 24"><path d="M8 5.5v13l11-6.5z" fill="currentColor" stroke="none" /></symbol>' +
    '<symbol id="i-link" viewBox="0 0 24 24">' +
    '<path d="M10.5 13.5a3.5 3.5 0 0 0 5 0l2.5-2.5a3.5 3.5 0 0 0-5-5l-1.5 1.5" />' +
    '<path d="M13.5 10.5a3.5 3.5 0 0 0-5 0L6 13a3.5 3.5 0 0 0 5 5l1.5-1.5" />' +
    '</symbol>' +
    '<symbol id="i-refresh" viewBox="0 0 24 24"><path d="M20 11a8 8 0 1 0-2.3 6.7" /><path d="M20 4v7h-7" /></symbol>' +
    '<symbol id="i-history" viewBox="0 0 24 24">' +
    '<path d="M4 12a8 8 0 1 0 2.3-5.7" /><path d="M4 4v6h6" />' +
    '<path d="M12 8v4.5l3 1.8" />' +
    '</symbol>' +
    '<symbol id="i-spark" viewBox="0 0 24 24">' +
    '<path d="M12 3.5 13.8 9l5.7 1.8-5.7 1.8L12 18.5l-1.8-5.9L4.5 10.8 10.2 9 12 3.5Z" />' +
    '</symbol>' +
    '<symbol id="i-layers" viewBox="0 0 24 24">' +
    '<path d="m12 3.5 8.5 4.5L12 12.5 3.5 8 12 3.5Z" />' +
    '<path d="m4 12 8 4.3 8-4.3" /><path d="m4 16 8 4.3 8-4.3" />' +
    '</symbol>' +
    '</defs></svg>';

  function inject() {
    if (document.getElementById('mr-icon-sprite')) return;
    var holder = document.createElement('div');
    holder.id = 'mr-icon-sprite';
    holder.setAttribute('aria-hidden', 'true');
    holder.style.cssText = 'position:absolute;width:0;height:0;overflow:hidden';
    holder.innerHTML = SPRITE;
    document.body.insertBefore(holder, document.body.firstChild);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', inject);
  } else {
    inject();
  }
})();
