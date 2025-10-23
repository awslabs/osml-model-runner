module.exports = {
  root: true,
  env: {
    node: true,
    es2020: true,
  },
  extends: [
    "eslint:recommended",
    "prettier",
  ],
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaVersion: 2020,
    sourceType: "module",
  },
  plugins: [
    "@typescript-eslint",
    "prettier",
  ],
  rules: {
    "prettier/prettier": "error",
    "@typescript-eslint/no-unused-expressions": [
      "error",
      { allowTernary: true }
    ],
    "@typescript-eslint/interface-name-prefix": "off",
    "@typescript-eslint/no-empty-interface": "off",
    "@typescript-eslint/no-inferrable-types": "off",
    "@typescript-eslint/no-non-null-assertion": "off",
    "@typescript-eslint/no-empty-function": "off",
  },
  overrides: [
    {
      files: ["**/*.test.ts", "**/*.spec.ts"],
      env: {
        jest: true,
      },
    },
  ],
  ignorePatterns: [
    "node_modules/",
    "cdk.out/",
    "*.js",
    "*.d.ts",
    "coverage/",
    "dist/",
    "build/",
  ],
};
