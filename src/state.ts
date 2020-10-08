import * as pull from 'pull-stream'
import { trueToNull } from './utils'

export type OnEnd = (endOrError: pull.EndOrError) => void
export interface StateOption {
  onEnd?: OnEnd
}

export class State {
  private _ending: pull.EndOrError = false
  private _aborting: pull.Abort = false
  private _endReason: pull.EndOrError = false

  constructor(private _opts: StateOption = {}) {}

  get finished() {
    return this._endReason
  }

  get finishing() {
    return this._ending || this._aborting
  }

  get ending() {
    return this._ending
  }

  get aborting() {
    return this._aborting
  }

  get normal() {
    return !this.finishing && !this.finished
  }

  askEnd(request: pull.EndOrError = true) {
    if (this.finished || this._ending || this._aborting) {
      return false
    }

    this._ending = request
    return true
  }

  askAbort(request: pull.Abort = true) {
    if (this.finished || this._aborting) {
      // even in the ending state, abort has higher priority.
      return false
    }

    this._ending = false
    this._aborting = request
    return true
  }

  ended(request: pull.EndOrError = true) {
    const isFirst = !this._endReason
    if (isFirst) {
      this._ending = false
      this._aborting = false
      this._endReason = request
      this._opts.onEnd?.(trueToNull(request))
    }
    return isFirst
  }
}
