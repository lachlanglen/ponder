import fs from "fs";
import type { GraphQLSchema } from "graphql";
import { printSchema } from "graphql";

import { toolConfig } from "./config";

const header = `
/* Autogenerated file. Do not edit manually. */
`;

const generateSchema = async (gqlSchema: GraphQLSchema) => {
  let generatedFileCount = 0;

  const body = printSchema(gqlSchema);

  const final = header + body;

  fs.writeFileSync(
    `${toolConfig.pathToGeneratedDir}/schema.graphql`,
    final,
    "utf8"
  );
  generatedFileCount += 1;

  return generatedFileCount;
};

export { generateSchema };
