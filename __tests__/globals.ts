const chai   = require('chai');
const sinon  = require('sinon');

(global as any).expect = chai.expect;
(global as any).spy    = sinon.spy;
(global as any).stub   = sinon.stub;
(global as any).match  = sinon.match;
(global as any).mock   = sinon.mock;

chai.use(require('chai-as-promised'));
chai.use(require('sinon-chai'));
chai.should();

import log from 'loglevel';
log.setLevel('silent')
