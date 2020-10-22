"use strict";
const watchdog = require('./watchdog');

// Test helpers
//------------------------------------------------------

// Create promise that resolves to <value> after <millis>
function delayedResolve(millis, value) {
    return new Promise((resolve, _) => {
        setTimeout(resolve, millis, value);
    });
}

// Create promise that rejects with <msg> after <millis>
function timeBomb(millis, msg) {
    return new Promise((_, reject) => {
        setTimeout(reject, millis, msg);
    });
}

// Test cases
//------------------------------------------------------

test('scenario #1: <promise> resolves first', () => {
    expect(
        watchdog(delayedResolve(1000, "the answer is 42"), 2000)
    ).resolves.toBe("the answer is 42");
});

test('scenario #2: <promise> rejects first', () => {
    expect(
        watchdog(timeBomb(1000, "BOOM!"), 2000)
    ).rejects.toEqual({
        timeout: false,
        err: "BOOM!",
    });
});

test('scenario #3: <millis> elapses first', () => {
    expect(
        watchdog(delayedResolve(1000, "the answer is 42"), 500)
    ).rejects.toEqual({
        timeout: true,
    });
});
