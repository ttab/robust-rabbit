module.exports = {
    transform: {
        ".(ts|tsx)": "ts-jest",
    },
    testRegex: "(/__tests__/.*|\\.(test|spec))\\.(ts|tsx|js)$",
    testPathIgnorePatterns: ['globals.js'],
    moduleFileExtensions: ["ts", "tsx", "js", "json"],
    setupFilesAfterEnv: ['jest-sinon', '<rootDir>/__tests__/globals.js']
};
