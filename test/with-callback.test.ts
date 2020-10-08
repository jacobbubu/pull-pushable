import * as pull from 'pull-stream'
import { pushable } from '../src'
import { getProbe } from './common'

const delay = (ms: number) => new Promise(_ => setTimeout(_, ms))

describe('with-callback', () => {
  it('push with callback', done => {
    const probe = getProbe()
    const buf = pushable()
    const callback = jest.fn(err => err || Date.now() - startTime)

    const source = pull(buf, probe)

    pull(
      source,
      pull.asyncMap(async (data, cb) => {
        await delay(100)
        cb(null, data)
      }),
      pull.take(2),
      pull.collect((err, results) => {
        probe.terminate()
        expect(err).toBeFalsy()
        expect(results).toEqual([1, 2])
        expect(callback).toBeCalledTimes(3)
        expect(callback.mock.results[0].value).toBeLessThan(20)
        expect(callback.mock.results[1].value).toBeGreaterThan(90)
        expect(callback.mock.results[2].value).toBe(true)
        done()
      })
    )

    const startTime = Date.now()
    buf.push(1, callback)
    buf.push(2, callback)
    buf.push(3, callback)
    buf.end()
  })
})
