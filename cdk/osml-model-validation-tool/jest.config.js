module.exports = {
  testEnvironment: "node",
  testMatch: ["**/*.test.ts"],
  transform: {
    "^.+\\.tsx?$": "ts-jest"
  },
  moduleNameMapper: {
    "^@/(.*)$": "<rootDir>/lib/$1"
  },
  transformIgnorePatterns: ["<rootDir>/node_modules/"],
  collectCoverage: true,
  collectCoverageFrom: [
    "**/*.{ts,tsx}",
    "!**/node_modules/**",
    "!**/vendor/**",
    "!**/dist/**",
    "!**/test_account.ts"
  ],
  coverageThreshold: {
    global: {
      lines: 90
    }
  },
  coverageReporters: ["html"],
  coverageDirectory: "<rootDir>/coverage",
  verbose: true,
  // Only include tests in the CDK project directory
  roots: [
    "<rootDir>"
  ]
};
