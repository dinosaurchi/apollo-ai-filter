import { z } from "zod";

const StepInputSchema = z
  .object({
    source: z.enum(["normalized", "prev_step"]).optional(),
    where: z
      .object({
        field: z.string().min(1),
        equals: z.string()
      })
      .optional()
  })
  .optional();

const FilterRuleSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("code_in"),
    field: z.string().min(1),
    values: z.array(z.string())
  }),
  z.object({
    type: z.literal("regex"),
    field: z.string().min(1),
    pattern: z.string().min(1),
    flags: z.string().optional()
  }),
  z.object({
    type: z.literal("contains_any"),
    field: z.string().min(1),
    values: z.array(z.string()),
    case_insensitive: z.boolean().optional()
  }),
  z.object({
    type: z.literal("equals_any"),
    field: z.string().min(1),
    values: z.array(z.string()),
    case_insensitive: z.boolean().optional()
  }),
  z.object({
    type: z.literal("not_empty"),
    field: z.string().min(1)
  }),
  z.object({
    type: z.literal("is_empty"),
    field: z.string().min(1)
  })
]);

const FilterStepSchema = z.object({
  id: z.string().min(1),
  type: z.literal("filter"),
  input: StepInputSchema,
  rules: z.object({
    keep_if_any: z.array(FilterRuleSchema).optional(),
    drop_if_any: z.array(FilterRuleSchema).optional()
  })
});

const AiTaskSchema = z.object({
  criteria_name: z.string().min(1),
  read_fields: z.array(z.string().min(1)).min(1),
  instructions: z.array(z.string()).min(1),
  label_set: z.array(z.string()).optional(),
  confidence_set: z.array(z.string()).optional(),
  decision_field: z.string().min(1),
  confidence_field: z.string().min(1),
  reason_field: z.string().optional(),
  evidence_field: z.string().optional()
});

const AiTextStepSchema = z.object({
  id: z.string().min(1),
  type: z.literal("ai_text"),
  input: StepInputSchema,
  ai: z.object({
    serverUrl: z.string().optional(),
    model: z.string().optional(),
    agent: z.string().optional(),
    concurrency: z.number().int().positive().optional(),
    session_concurrency: z.number().int().positive().optional(),
    batch_size: z.number().int().positive().optional(),
    max_attempts: z.number().int().positive().optional(),
    retry_delay_ms: z.number().int().nonnegative().optional(),
    continue_session_id: z.string().optional()
  }),
  task: AiTaskSchema,
  routing: z
    .object({
      finalize_if: z
        .object({
          all: z.array(z.object({ field: z.string().min(1), equals: z.string() })).optional()
        })
        .optional()
    })
    .optional()
});

const WebAiStepSchema = z.object({
  id: z.string().min(1),
  type: z.literal("web_ai"),
  input: StepInputSchema,
  scrape: z.object({
    enabled: z.boolean().optional(),
    concurrency: z.number().int().positive().optional(),
    max_pages_per_domain: z.number().int().positive().optional(),
    url_paths: z.array(z.string()).optional(),
    timeout_ms: z.number().int().positive().optional(),
    cache_by_domain: z.boolean().optional(),
    user_agent: z.string().optional(),
    max_chars_per_page: z.number().int().positive().optional(),
    max_total_chars: z.number().int().positive().optional()
  }),
  ai: z.object({
    serverUrl: z.string().optional(),
    model: z.string().optional(),
    agent: z.string().optional(),
    concurrency: z.number().int().positive().optional(),
    batch_size: z.number().int().positive().optional(),
    max_attempts: z.number().int().positive().optional(),
    retry_delay_ms: z.number().int().nonnegative().optional(),
    browse_fallback_enabled: z.boolean().optional(),
    browse_fallback_min_chars: z.number().int().nonnegative().optional(),
    browse_fallback_batch_size: z.number().int().positive().optional(),
    browse_fallback_concurrency: z.number().int().positive().optional(),
    browse_fallback_agent: z.string().optional(),
    browse_fallback_model: z.string().optional(),
    browse_fallback_max_attempts: z.number().int().positive().optional(),
    browse_fallback_retry_delay_ms: z.number().int().nonnegative().optional(),
    continue_session_id: z.string().optional()
  }),
  task: AiTaskSchema
});

const ApolloPeopleStepSchema = z.object({
  id: z.string().min(1),
  type: z.literal("apollo_people"),
  from_final: z
    .object({
      output_csv: z.string().optional(),
      decision_field: z.string().optional(),
      decision_equals: z.string().optional(),
      confidence_field: z.string().optional(),
      confidence_equals: z.string().optional()
    })
    .optional(),
  company: z
    .object({
      id_field: z.string().optional(),
      name_field: z.string().optional(),
      domain_field: z.string().optional()
    })
    .optional(),
  people: z.object({
    per_page: z.number().int().positive().optional(),
    max_pages: z.number().int().positive().optional(),
    people_limit_per_company: z.number().int().positive().optional(),
    target_roles_or_titles: z.array(z.string().min(1)).min(1),
    seniority_min: z.enum(["IC", "Manager", "Director", "VP", "CLevel"]),
    person_locations: z.array(z.string()).optional(),
    q_keywords: z.array(z.string()).optional(),
    max_company_ids_per_request: z.number().int().positive().optional()
  }),
  rate_limit: z
    .object({
      request_delay_ms: z.number().int().nonnegative().optional(),
      duplicate_company_delay_ms: z.number().int().nonnegative().optional(),
      max_attempts: z.number().int().positive().optional(),
      retry_base_delay_ms: z.number().int().positive().optional()
    })
    .optional()
});

const StepSchema = z.discriminatedUnion("type", [
  FilterStepSchema,
  AiTextStepSchema,
  WebAiStepSchema,
  ApolloPeopleStepSchema
]);

export const RunConfigSchema = z.object({
  run: z
    .object({
      name: z.string().optional(),
      id_field: z.string().optional()
    })
    .optional(),
  io: z
    .object({
      output_root: z.string().optional(),
      copy_input_csv: z.boolean().optional()
    })
    .optional(),
  normalize: z
    .object({
      trim_all_strings: z.boolean().optional(),
      derive: z
        .object({
          domain_from: z.string().optional(),
          profile_text_fields: z.array(z.string()).optional()
        })
        .optional()
    })
    .optional(),
  steps: z.array(StepSchema).min(1),
  finalize: z
    .object({
      output_csv: z.string().optional(),
      include_not_sure_in_a3: z.boolean().optional(),
      views: z
        .array(
          z.object({
            name: z.string().min(1),
            where: z.object({ field: z.string().min(1), equals: z.string() })
          })
        )
        .optional()
    })
    .optional()
});

export type RunConfig = z.infer<typeof RunConfigSchema>;

export function collectRequiredInputFields(cfg: RunConfig): string[] {
  const generated = new Set<string>(["__row_id", "domain", "profile_text", "Decision-Final", "Confidence-Final", "Final-Source"]);
  for (const step of cfg.steps) {
    if (step.type === "ai_text" || step.type === "web_ai") {
      generated.add(step.task.decision_field);
      generated.add(step.task.confidence_field);
      if (step.task.reason_field) generated.add(step.task.reason_field);
      if (step.task.evidence_field) generated.add(step.task.evidence_field);
    }
  }

  const out = new Set<string>();
  // run.id_field is optional in practice because the analyzer falls back to __row_id.
  if (cfg.normalize?.derive?.domain_from) out.add(cfg.normalize.derive.domain_from);
  for (const field of cfg.normalize?.derive?.profile_text_fields ?? []) out.add(field);
  for (const step of cfg.steps) {
    if (step.type === "filter") {
      for (const rule of step.rules.keep_if_any ?? []) if (!generated.has(rule.field)) out.add(rule.field);
      for (const rule of step.rules.drop_if_any ?? []) if (!generated.has(rule.field)) out.add(rule.field);
      if (step.input?.where?.field && !generated.has(step.input.where.field)) out.add(step.input.where.field);
    }
    if (step.type === "ai_text" || step.type === "web_ai") {
      for (const field of step.task.read_fields) if (!generated.has(field)) out.add(field);
      if (step.input?.where?.field && !generated.has(step.input.where.field)) out.add(step.input.where.field);
    }
  }
  return Array.from(out).filter((x) => x.trim().length > 0);
}
