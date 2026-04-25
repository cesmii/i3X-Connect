// i3X Viz — Service Worker
// Strategy: network-first, fall back to cache.
// This ensures deployed updates are always picked up when online,
// while the app still works offline after the first visit.

const CACHE = 'i3x-viz-v1';

const PRECACHE = [
  './',
  './index.html',
  './icon.svg',
  './manifest.json',
  './i3XHero.png',
  './favicon-highbyte.png',
  './favicon-inductive.png',
  './favicon-timebase.ico',
  './favicon-postgres.ico',
  './favicon-cesmii.png',
  './favicon-litmus.ico',
];

self.addEventListener('install', e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(PRECACHE)));
  self.skipWaiting();
});

self.addEventListener('activate', e => {
  // Delete any old cache versions
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', e => {
  // Only handle GET requests for same-origin or precached assets
  if (e.request.method !== 'GET') return;

  e.respondWith(
    fetch(e.request)
      .then(response => {
        // Cache a clone of successful responses
        if (response.ok) {
          const clone = response.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
        }
        return response;
      })
      .catch(() => caches.match(e.request))
  );
});
