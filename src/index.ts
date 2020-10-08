import * as pull from 'pull-stream'
import { Debug } from '@jacobbubu/debug'
import { State } from './state'

const getPushableName = (function() {
  let counter = 1
  return () => (counter++).toString()
})()

type OnClose = (err?: pull.EndOrError) => void

const DefaultLogger = Debug.create('pushable')

export { State }

export enum BufferItemIndex {
  Data = 0,
  Cb
}

export type BufferItemCallback = (endOrError: pull.EndOrError) => void
export type BufferItem<T> = [T?, BufferItemCallback?]

export interface Read<T> {
  (endOrError: pull.Abort, cb: pull.SourceCallback<T>): void
  end: (end?: pull.EndOrError) => void
  abort: (end?: pull.EndOrError) => void
  push: (data: T, bufferedCb?: BufferItemCallback) => void
  buffer: BufferItem<T>[]
}
export function pushable<T>(name?: string | OnClose, onclose?: OnClose): Read<T> {
  let _name: string
  let _onclose: OnClose | undefined
  let _buffer: BufferItem<T>[] = []
  let _reentered = 0

  if (typeof name === 'function') {
    _onclose = name
    name = undefined
  } else {
    _onclose = onclose
  }

  const _sourceState = new State({ onEnd: _onclose })
  let _cbs: pull.SourceCallback<T>[] = []

  _name = name || getPushableName()
  let logger = DefaultLogger.ns(_name)

  const end = (end?: pull.EndOrError) => {
    if (!_sourceState.askEnd(end)) return

    logger.debug('end(end=%o) has been called', end)
    drain()
  }

  const abort = (end?: pull.EndOrError) => {
    if (!_sourceState.askAbort(end)) return

    logger.debug('abort(end=%o) has been called', end)
    drain()
  }

  const push = (data: T, bufferedCb?: BufferItemCallback) => {
    logger.info('push(data=%o), ended: %o', data, _sourceState)
    if (!_sourceState.normal) return false

    _buffer.push([data, bufferedCb])
    drain()
    return true
  }

  const read: Read<T> = (abort: pull.Abort, cb: pull.SourceCallback<T>) => {
    logger.info('read(abort=%o)', abort)
    if (_sourceState.finished) {
      return cb(_sourceState.finished)
    }

    _cbs.push(cb)

    if (abort) {
      _sourceState.askAbort(abort)
    }
    drain()
  }

  read.end = end
  read.abort = abort
  read.push = push
  read.buffer = _buffer

  const drainAbort = () => {
    if (!_sourceState.aborting || _reentered > 0) return false
    _reentered++

    try {
      const abort = _sourceState.aborting
      // in case there's still data in the _buffer
      while (_buffer.length > 0) {
        _buffer.shift()?.[BufferItemIndex.Cb]?.(abort)
      }

      // call of all waiting callback functions
      while (_cbs.length > 0) {
        _cbs.shift()?.(abort)
      }

      _sourceState.ended(abort)
    } finally {
      _reentered--
    }
    return true
  }

  const drainNormal = () => {
    if (_reentered > 0) return

    _reentered++
    try {
      while (_buffer.length > 0) {
        const cb = _cbs.shift()
        if (cb) {
          const bufferItem = _buffer.shift()!
          cb(null, bufferItem[BufferItemIndex.Data])
          bufferItem[BufferItemIndex.Cb]?.(null)
        } else {
          break
        }
      }
    } finally {
      _reentered--
    }
  }

  const drainEnd = () => {
    if (!_sourceState.ending || _reentered > 0) return
    _reentered++

    try {
      const end = _sourceState.ending
      // more cb is needed to satisfy the buffer
      if (_buffer.length > 0) {
        _reentered--
        return
      }

      // call of all waiting callback functions
      while (_cbs.length > 0) {
        _cbs.shift()?.(end)
      }

      _sourceState.ended(end)
    } finally {
      _reentered--
    }
  }

  const drain = () => {
    if (drainAbort()) return

    drainNormal()
    if (drainAbort()) return

    drainEnd()
    if (drainAbort()) return
  }

  return read
}
