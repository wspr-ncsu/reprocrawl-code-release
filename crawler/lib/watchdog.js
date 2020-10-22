"use strict";

// Wrap <promise> inside a watchdog Promise that will resolve/reject in at most <millis>
// Scenarios:
// * <promise> resolves before <millis> elapsed => watchdog resolves to the same result (timeout cancelled)
// * <promise> rejects before <millis> elapsed => watchdog rejects with `{watchdog: false, err: <error_from_promise>}` (timeout cancelled)
// * <millis> elapses => watchdog rejects with `{watchdog: true}` (<promise> may or may not resolve in the future)
function promiseWatchdog(promise, millis) {
    let token;
    const timeout = new Promise((_, reject) => {
        token = setTimeout(reject, millis, { timeout: true });
    });
    timeout.cancel = function () {
        clearTimeout(token);
    };
    return Promise.race([
        timeout,
        promise.then(result => {
            timeout.cancel();
            return Promise.resolve(result);
        }).catch(err => {
            timeout.cancel();
            return Promise.reject({ timeout: false, err });
        }),
    ]);
}
module.exports = promiseWatchdog;