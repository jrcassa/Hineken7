// eslint.config.mjs
import js from "@eslint/js";
import tseslint from "typescript-eslint";
import prettier from "eslint-plugin-prettier";
import { FlatCompat } from "@eslint/eslintrc";

const compat = new FlatCompat({
    baseDirectory: import.meta.dirname,
    recommendedConfig: js.configs.recommended,
});

export default [

    // Arquivos ignorados
    {
        ignores: [
            "src/Tests/*",
            "**/lib",
            "**/coverage",
            "**/*.lock",
            "**/.eslintrc.json",
            "src/WABinary/index.ts",
            "**/WAProto",
            "Example/Example.ts",
            "**/docs",
            "**/proto-extract"
        ]
    },

    // Regras JS padrão
    js.configs.recommended,

    // Regras TS do typescript-eslint
    ...tseslint.configs.recommended,
    ...tseslint.configs.recommendedTypeChecked,

    // Config TS especializada
    {
        files: ["**/*.ts", "**/*.tsx"],

        languageOptions: {
            parser: tseslint.parser,
            parserOptions: {
                project: "./tsconfig.json",
            },
        },

        plugins: {
            "@typescript-eslint": tseslint.plugin,
            prettier,
        },

        rules: {
            // Regras recomendadas pelo typescript-eslint já ativadas acima

            camelcase: "off",
            indent: "off",

            "@typescript-eslint/no-explicit-any": ["warn", {
                ignoreRestArgs: true
            }],

            "space-before-function-paren": ["error", {
                anonymous: "always",
                named: "never",
                asyncArrow: "always"
            }],

            "@typescript-eslint/no-unused-vars": ["error", {
                caughtErrors: "none"
            }],

            // Regras do prettier
            "prettier/prettier": "error"
        }
    },

    // Config Prettier
    ...compat.extends("plugin:prettier/recommended"),
];
