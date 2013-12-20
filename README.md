# GemeraldBeanstalk
[![Build Status](https://travis-ci.org/gemeraldbeanstalk/gemerald_beanstalk.png?branch=master)](https://travis-ci.org/gemeraldbeanstalk/gemerald_beanstalk)
[![Coverage Status](https://coveralls.io/repos/gemeraldbeanstalk/gemerald_beanstalk/badge.png)](https://coveralls.io/r/gemeraldbeanstalk/gemerald_beanstalk)
[![Code Climate](https://codeclimate.com/github/gemeraldbeanstalk/gemerald_beanstalk.png)](https://codeclimate.com/github/gemeraldbeanstalk/gemerald_beanstalk)
[![Dependency Status](https://gemnasium.com/gemeraldbeanstalk/gemerald_beanstalk.png)](https://gemnasium.com/gemeraldbeanstalk/gemerald_beanstalk)


GemeraldBeanstalk offers a Ruby implementation of beanstalkd for testing and other uses.

## Usage

GemeraldBeanstalk should work as a drop in replacement for beanstalkd. You can
start a server via GemeraldBeanstalk::Server.start:
```ruby

  # Start a GemeraldBeanstalk bound to 0.0.0.0:11300
  GemeraldBeanstalk::Server.start

  # Customize server binding
  GemeraldBeanstalk::Server.start('192.168.1.10', 11301)
```

GemeraldBeanstalk::Server.start returns an array containing the Thread the
server is running in and the server's GemeraldBeanstalk::Beanstalk instance.

The internals of GemeraldBeanstalk are undocumented at this point, with the
expectation being that it should be interacted with strictly via the [beanstalkd
protocol](https://github.com/kr/beanstalkd/blob/master/doc/protocol.md). This
will likely change in the future, allowing more programatic access directly to
the GemeraldBeanstalk::Beanstalk.

## Installation

Add this line to your application's Gemfile:

    gem 'gemerald_beanstalk'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install gemerald_beanstalk

## Unbugs!
In the process of building GemeraldBeanstalk, a number of bugs and inconsistencies
with Beanstalkd protocol were discovered. Patches have been submitted to correct
the various bugs and inconsistencies, but they have not yet been merged into
beanstalkd.

It would be fairly tedious to reproduce the behavior of some of the bugs, and as
such, GemeraldBeanstalk doesn't suffer from them. This can be troubling when
you run tests that work against GemeraldBeanstalk, but then fail against an
actual beanstalkd server. Below are a list of those protocol issues that exist
with Beanstalk, but not with GemeraldBeanstalk.
 * [Pause tube should check tube name valid](https://github.com/kr/beanstalkd/pull/217)
 * [Can't ignore tube with name 200 chars long](https://github.com/kr/beanstalkd/issues/212)
 * [Use of 200-char tube name causes INTERNAL_ERROR](https://github.com/kr/beanstalkd/issues/211)

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
