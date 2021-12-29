// use for azure blob to s3 file transfer

#!/usr/bin/env node
const inquirer = require("inquirer");
const fs = require("fs");
const beautify = require('beautify');

const enterValues = () => {
  const input_values = [
    {
      name: "aws_region",
      type: "input",
      message: "Enter AWS Region:"
    },
    {
      type: "input",
      name: "bucket_name",
      message: "Enter S3 Bucket Name:"
    },
    {
      type: "input",
      name: "azure_connection",
      message: "Enter Azure Connection String:"
    },
    {
      type: "input",
      name: "azure_container",
      message: "Enter Azure Container:"
    }
  ];
  return inquirer.prompt(input_values);
};

const createFile = (aws_region, bucket_name, azure_connection, azure_container) => {

  const code = `var toS3 = require('azure-blob-to-s3')` +
                `\n` +
                `\n` +
                `toS3({` +
                  `aws: {` +
                    `region: "${aws_region}",` +
                    `bucket: "${bucket_name}"` +
                  `},` +
                  `azure: {` +
                    `connection: "${azure_connection}",` +
                    `container: "${azure_container}"` +
                  `}` +
                `})`;
  const data = beautify(code, {format: 'js'});
  fs.writeFile('server.js', data, function (err) {
    if (err) throw err;
    console.log('File is created successfully.');
  });

};


const run = async () => {

  const values = await enterValues();

  const { aws_region, bucket_name, azure_connection, azure_container } = values;

  const filePath = createFile(aws_region.trim(), bucket_name.trim(), azure_connection.trim(), azure_container.trim());

};

            run();	