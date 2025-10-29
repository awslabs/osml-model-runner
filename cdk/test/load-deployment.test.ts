/*
 * Copyright 2025 Amazon.com, Inc. or its affiliates.
 */

/**
 * Unit tests for loadDeploymentConfig function.
 */

// Mock fs module before importing the function under test
jest.mock("fs", () => {
  const actualFs = jest.requireActual("fs");
  return {
    ...actualFs,
    existsSync: jest.fn(),
    readFileSync: jest.fn()
  };
});

import { existsSync, readFileSync } from "fs";

import { loadDeploymentConfig } from "../bin/deployment/load-deployment";

describe("loadDeploymentConfig", () => {
  beforeEach(() => {
    // Reset mocks
    jest.clearAllMocks();
    (existsSync as jest.Mock).mockReturnValue(true);
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  test("loads valid deployment configuration", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    const result = loadDeploymentConfig();

    expect(result.projectName).toBe("test-project");
    expect(result.account.id).toBe("123456789012");
    expect(result.account.region).toBe("us-west-2");
    expect(result.account.prodLike).toBe(false);
    expect(result.account.isAdc).toBe(false);
  });

  test("throws error when deployment.json is missing", () => {
    (existsSync as jest.Mock).mockReturnValue(false);

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/Missing deployment.json file/);
  });

  test("throws error when JSON is invalid", () => {
    (readFileSync as jest.Mock).mockReturnValue("{ invalid json }");

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/Invalid JSON format/);
  });

  test("validates required projectName field", () => {
    const config = {
      account: {
        id: "123456789012",
        region: "us-west-2"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/Missing required field: projectName/);
  });

  test("validates projectName is not empty", () => {
    const config = {
      projectName: "",
      account: {
        id: "123456789012",
        region: "us-west-2"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/cannot be empty/);
  });

  test("validates required account.id field", () => {
    const config = {
      projectName: "test-project",
      account: {
        region: "us-west-2"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/Missing required field: account.id/);
  });

  test("validates account ID format (must be 12 digits)", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "12345",
        region: "us-west-2"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/Invalid AWS account ID format/);
  });

  test("validates required account.region field", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/Missing required field: account.region/);
  });

  test("validates region format", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "invalid_region_123"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/Invalid AWS region format/);
  });

  test("loads prodLike and isAdc flags", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2",
        prodLike: true,
        isAdc: true
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    const result = loadDeploymentConfig();

    expect(result.account.prodLike).toBe(true);
    expect(result.account.isAdc).toBe(true);
  });

  test("defaults prodLike and isAdc to false when not specified", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    const result = loadDeploymentConfig();

    expect(result.account.prodLike).toBe(false);
    expect(result.account.isAdc).toBe(false);
  });

  test("validates VPC ID format when provided", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2"
      },
      networkConfig: {
        vpcId: "invalid-vpc-id"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/Invalid VPC ID format/);
  });

  test("validates security group ID format when provided", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2"
      },
      networkConfig: {
        vpcId: "vpc-12345678",
        targetSubnets: ["subnet-12345"],
        securityGroupId: "invalid-sg-id"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/Invalid security group ID format/);
  });

  test("requires targetSubnets when vpcId is provided", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2"
      },
      networkConfig: {
        vpcId: "vpc-12345678"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/targetSubnets must also be specified/);
  });

  test("validates targetSubnets is array when provided", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2"
      },
      networkConfig: {
        vpcId: "vpc-12345678",
        targetSubnets: "not-an-array"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    expect(() => {
      loadDeploymentConfig();
    }).toThrow(/must be an array/);
  });

  test("loads networkConfig with valid VPC configuration", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2"
      },
      networkConfig: {
        vpcId: "vpc-12345678",
        targetSubnets: ["subnet-12345", "subnet-67890"],
        securityGroupId: "sg-1234567890abcdef0"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    const result = loadDeploymentConfig();

    expect(result.networkConfig).toBeDefined();
    expect(result.networkConfig?.VPC_ID).toBe("vpc-12345678");
    expect(result.networkConfig?.TARGET_SUBNETS).toEqual([
      "subnet-12345",
      "subnet-67890"
    ]);
    expect(result.networkConfig?.SECURITY_GROUP_ID).toBe(
      "sg-1234567890abcdef0"
    );
  });

  test("loads dataplaneConfig when provided", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2"
      },
      dataplaneConfig: {
        CONTAINER_URI: "test-container:latest",
        ECS_TASK_CPU: 1024
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    const result = loadDeploymentConfig();

    expect(result.dataplaneConfig).toEqual({
      CONTAINER_URI: "test-container:latest",
      ECS_TASK_CPU: 1024
    });
  });

  test("loads deployIntegrationTests flag", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2"
      },
      deployIntegrationTests: true
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    const result = loadDeploymentConfig();

    expect(result.deployIntegrationTests).toBe(true);
  });

  test("defaults deployIntegrationTests to false when not specified", () => {
    const config = {
      projectName: "test-project",
      account: {
        id: "123456789012",
        region: "us-west-2"
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    const result = loadDeploymentConfig();

    expect(result.deployIntegrationTests).toBe(false);
  });

  test("trims whitespace from string fields", () => {
    const config = {
      projectName: "  test-project  ",
      account: {
        id: "  123456789012  ",
        region: "  us-west-2  "
      }
    };

    (readFileSync as jest.Mock).mockReturnValue(JSON.stringify(config));

    const result = loadDeploymentConfig();

    expect(result.projectName).toBe("test-project");
    expect(result.account.id).toBe("123456789012");
    expect(result.account.region).toBe("us-west-2");
  });
});
