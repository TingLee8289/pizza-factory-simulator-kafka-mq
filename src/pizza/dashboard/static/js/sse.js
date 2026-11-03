// SSE client: two EventSource streams feeding the store. EventSource retries on its own.
export function connect(store, { baseUrl = "" } = {}) {
  const stats = { malformed: 0 };

  function handler(fn) {
    return (ev) => {
      store.noteData();
      let data;
      try {
        data = JSON.parse(ev.data);
      } catch (_err) {
        stats.malformed += 1;
        return;
      }
      fn(data);
    };
  }

  const state = new EventSource(`${baseUrl}/api/stream/state`);
  state.addEventListener("snapshot", handler((d) => store.replace(d)));
  state.addEventListener("tick", handler((d) => store.merge(d)));

  const events = new EventSource(`${baseUrl}/api/stream/events`);
  events.addEventListener("fact", handler((d) => store.applyFact(d)));
  events.addEventListener("comparison", handler((d) => store.applyComparison(d)));
  events.addEventListener("comparison_result", handler((d) => store.applyComparisonResult(d)));

  for (const es of [state, events]) {
    es.onopen = () => {
      store.noteData();
      store.setConnection("live");
    };
    es.onerror = () => store.setConnection("reconnecting");
  }

  return {
    stats,
    close() {
      state.close();
      events.close();
    },
  };
}
