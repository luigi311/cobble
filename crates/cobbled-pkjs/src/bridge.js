(() => {
  "use strict";

  globalThis.window = globalThis;
  globalThis.navigator = globalThis.navigator || {};

  const listeners = new Map();
  const appMessageCallbacks = new Map();
  const locationCallbacks = new Map();
  const timerCallbacks = new Map();
  let nextTimerId = 1;

  function dispatch(type, detail) {
    const callbacks = listeners.get(type);
    if (!callbacks) return true;
    let succeeded = true;
    for (const callback of [...callbacks]) {
      try {
        callback(Object.assign({ type }, detail || {}));
      } catch (error) {
        console.error(`PKJS ${type} handler failed`, error);
        succeeded = false;
      }
    }
    return succeeded;
  }

  function callSafely(label, callback, ...args) {
    if (typeof callback !== "function") return true;
    try {
      callback(...args);
      return true;
    } catch (error) {
      console.error(`${label} failed`, error);
      return false;
    }
  }

  function stringifyLog(args) {
    return args.map((value) => {
      if (value instanceof Error) {
        const message = String(value);
        const stack = value.stack ? String(value.stack) : "";
        return stack.includes(message) ? stack : [message, stack].filter(Boolean).join("\n");
      }
      if (typeof value === "object") {
        try { return JSON.stringify(value); } catch (_) { return "[object]"; }
      }
      return String(value);
    }).join(" ");
  }

  globalThis.console = {};
  for (const level of ["debug", "info", "log", "warn", "error"]) {
    console[level] = (...args) => __cobbleLog(level, stringifyLog(args));
  }

  let storage;
  try { storage = JSON.parse(__cobbleInitialStorage); } catch (_) { storage = {}; }
  function saveStorage() { __cobbleSaveStorage(JSON.stringify(storage)); }
  const storageApi = {
    get length() { return Object.keys(storage).length; },
    key(index) { return Object.keys(storage)[Number(index)] ?? null; },
    getItem(key) {
      key = String(key);
      return Object.prototype.hasOwnProperty.call(storage, key) ? storage[key] : null;
    },
    setItem(key, value) { storage[String(key)] = String(value); saveStorage(); },
    removeItem(key) { delete storage[String(key)]; saveStorage(); },
    clear() { storage = {}; saveStorage(); },
  };
  globalThis.localStorage = new Proxy(storageApi, {
    get(target, key) {
      if (key in target) {
        const value = target[key];
        return typeof value === "function" ? value.bind(target) : value;
      }
      return target.getItem(key);
    },
    set(target, key, value) { target.setItem(key, value); return true; },
    deleteProperty(target, key) { target.removeItem(key); return true; },
    ownKeys() { return Object.keys(storage); },
    getOwnPropertyDescriptor(target, key) {
      if (key in target || Object.prototype.hasOwnProperty.call(storage, key)) {
        return { enumerable: true, configurable: true };
      }
    },
  });

  globalThis.setTimeout = (callback, delay, ...args) => {
    const id = nextTimerId++;
    timerCallbacks.set(id, { callback, args, repeat: false });
    __cobbleScheduleTimer(id, Number(delay) || 0, false);
    return id;
  };
  globalThis.setInterval = (callback, delay, ...args) => {
    const id = nextTimerId++;
    timerCallbacks.set(id, { callback, args, repeat: true });
    __cobbleScheduleTimer(id, Number(delay) || 0, true);
    return id;
  };
  globalThis.clearTimeout = globalThis.clearInterval = (id) => {
    timerCallbacks.delete(Number(id));
    __cobbleCancelTimer(Number(id));
  };
  globalThis.__cobbleFireTimer = (id) => {
    const timer = timerCallbacks.get(id);
    if (!timer) return;
    if (!timer.repeat) timerCallbacks.delete(id);
    try { timer.callback(...timer.args); } catch (error) { console.error("timer failed", error); }
  };

  class XMLHttpRequest {
    constructor() {
      this.readyState = 0;
      this.status = 0;
      this.statusText = "";
      this.response = "";
      this.responseText = "";
      this.onreadystatechange = null;
      this.onload = null;
      this.onerror = null;
      this._headers = {};
    }
    open(method, url) { this._method = method; this._url = url; this.readyState = 1; }
    setRequestHeader(name, value) { this._headers[String(name)] = String(value); }
    send(body) {
      try {
        const result = JSON.parse(__cobbleHttpRequest(
          String(this._method || "GET"), String(this._url),
          JSON.stringify(this._headers), body == null ? "" : String(body)
        ));
        this.status = result.status || 0;
        this.statusText = result.status_text || "";
        this.response = this.responseText = result.body || "";
        this.readyState = 4;
        if (this.onreadystatechange) this.onreadystatechange();
        if (result.status) { if (this.onload) this.onload(); }
        else if (this.onerror) this.onerror(new Error(result.error || "HTTP request failed"));
      } catch (error) {
        this.readyState = 4;
        if (this.onerror) this.onerror(error);
      }
    }
    addEventListener(type, callback) { this[`on${type}`] = callback; }
    getResponseHeader() { return null; }
    getAllResponseHeaders() { return ""; }
  }
  XMLHttpRequest.DONE = 4;
  globalThis.XMLHttpRequest = XMLHttpRequest;

  globalThis.fetch = (url, options) => {
    options = options || {};
    const result = JSON.parse(__cobbleHttpRequest(
      String(options.method || "GET"), String(url),
      JSON.stringify(options.headers || {}), options.body == null ? "" : String(options.body)
    ));
    const response = {
      ok: !!result.ok,
      status: result.status || 0,
      statusText: result.status_text || "",
      text: () => Promise.resolve(result.body || ""),
      json: () => Promise.resolve(JSON.parse(result.body || "null")),
    };
    return result.status ? Promise.resolve(response) : Promise.reject(new Error(result.error || "HTTP request failed"));
  };

  globalThis.Pebble = {
    addEventListener(type, callback) {
      if (typeof callback !== "function") return;
      if (!listeners.has(type)) listeners.set(type, new Set());
      listeners.get(type).add(callback);
    },
    removeEventListener(type, callback) { listeners.get(type)?.delete(callback); },
    sendAppMessage(data, onSuccess, onFailure) {
      const id = __cobbleSendAppMessage(JSON.stringify(data || {}));
      if (id < 0) {
        if (onFailure) onFailure({ error: "Invalid AppMessage" });
        return id;
      }
      appMessageCallbacks.set(id, { onSuccess, onFailure });
      return id;
    },
    openURL(url) {
      __cobbleOpenUrl(String(url));
      return String(url);
    },
    getActiveWatchInfo() { return JSON.parse(__cobbleWatchInfo); },
    getAccountToken() { return ""; },
    getWatchToken() { return ""; },
  };

  navigator.geolocation = {
    getCurrentPosition(onSuccess, onError) {
      const id = __cobbleRequestLocation();
      locationCallbacks.set(id, { onSuccess, onError });
    },
    watchPosition(onSuccess, onError) {
      const id = __cobbleRequestLocation();
      locationCallbacks.set(id, { onSuccess, onError });
      return id;
    },
    clearWatch(id) { locationCallbacks.delete(Number(id)); },
  };

  globalThis.__cobbleReady = () => dispatch("ready", { ready: true });
  globalThis.__cobbleDispatch = (encoded) => {
    const command = JSON.parse(encoded);
    switch (command.type) {
      case "app_message":
        dispatch("appmessage", { payload: command.data || {} });
        break;
      case "app_message_result": {
        const callbacks = appMessageCallbacks.get(command.request_id);
        if (!callbacks) break;
        appMessageCallbacks.delete(command.request_id);
        const payload = { data: { transactionId: command.request_id } };
        if (command.success) {
          callSafely("AppMessage success callback", callbacks.onSuccess, payload);
          dispatch("appmessage_ack", { payload });
        } else {
          payload.error = "nack";
          callSafely("AppMessage failure callback", callbacks.onFailure, payload, "nack");
          dispatch("appmessage_nack", { payload });
        }
        break;
      }
      case "show_configuration":
        dispatch("showConfiguration", {});
        dispatch("settings_webui_allowed", {});
        break;
      case "webview_closed":
        dispatch("webviewclosed", { response: command.response });
        break;
      case "location_result": {
        const callbacks = locationCallbacks.get(command.request_id);
        if (!callbacks) break;
        locationCallbacks.delete(command.request_id);
        if (command.error) {
          callSafely("geolocation error callback", callbacks.onError, {
            code: 1, message: command.error,
          });
        } else {
          callSafely("geolocation success callback", callbacks.onSuccess, { coords: {
            latitude: command.latitude, longitude: command.longitude,
            accuracy: 0, altitude: null, heading: null, speed: null,
          }});
        }
        break;
      }
    }
  };
})();
