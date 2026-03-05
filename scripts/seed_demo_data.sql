INSERT INTO clients (id, name, phone, city, state, created_at)
VALUES
  ('CUST-001', 'Agro Norte', '+5565999001001', 'Cuiaba', 'MT', NOW()),
  ('CUST-002', 'Fazenda Sol', '+5565999001002', 'Rondonopolis', 'MT', NOW())
ON CONFLICT (id) DO NOTHING;

INSERT INTO machines (id, client_id, brand, model, serial, year, telemetry_status, telemetry_active, created_at)
VALUES
  ('MACH-001', 'CUST-001', 'CASE', 'CASE-580N', 'SN580A', 2023, 'inactive', FALSE, NOW()),
  ('MACH-002', 'CUST-002', 'CASE', 'CASE-770EX', 'SN770B', 2022, 'active', TRUE, NOW())
ON CONFLICT (id) DO NOTHING;

INSERT INTO machine_ownership (client_id, machine_id, start_at, end_at)
VALUES
  ('CUST-001', 'MACH-001', NOW(), NULL),
  ('CUST-002', 'MACH-002', NOW(), NULL)
ON CONFLICT DO NOTHING;

INSERT INTO offer_rules (id, rule_type, predicate, offer_template, sku_list, priority, enabled)
VALUES
  ('seed-parts-offer', 'machine', '{"state":"MT"}'::jsonb, 'Temos pecas para sua maquina.', '["ABC123","XYZ777"]'::jsonb, 10, TRUE)
ON CONFLICT (id) DO NOTHING;
