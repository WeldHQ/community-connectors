import { handleRequest } from "./handlers/requestHandler.mjs";
import { getSchema } from "./handlers/schemaHandler.mjs";
import { checkAuth } from "./handlers/authHandler.mjs";

export const handler = async (event) => {
  console.log(event);
  
  checkAuth(event);

  if (event.rawPath === "//schema") return getSchema();

  let endpoint = event.body && JSON.parse(event.body).name ? JSON.parse(event.body).name : null;

  if(endpoint === null) return null;
  
  return await handleRequest(event, process.env.RECHARGE_API_KEY, endpoint);
};
