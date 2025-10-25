/**
 * Jest setup file for testcontainers
 */

// Configure testcontainers to use Colima Docker socket
process.env['DOCKER_HOST'] =
  'unix:///Users/alanharrison/.colima/default/docker.sock';
process.env['TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE'] =
  '/Users/alanharrison/.colima/default/docker.sock';
process.env['TESTCONTAINERS_RYUK_DISABLED'] = 'true';

// Increase timeout for testcontainers
jest.setTimeout(60000);

// Global test setup
beforeAll(async () => {
  // Any global setup can go here
});

afterAll(async () => {
  // Any global cleanup can go here
});
