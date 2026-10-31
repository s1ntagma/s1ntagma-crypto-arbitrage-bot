---
name: room-layout-fixed-structure-check
description: Plan a furniture layout, render prompt or room description from a floor plan or measurements by first writing a fixed-structure table (walls, windows, doors with swing, units, each value marked given, derived or not given), placing furniture only from stated dimensions with clearance arithmetic shown, and listing every wall, opening or object that moved, appeared or vanished when a layout or generated image is compared back. Use when a user gives room dimensions, a plan or photos and wants furniture placed, a walkway checked, or a render checked against the real room. Not for ordinance or zoning checks, structural or safety advice, or inferring dimensions from one photo with no reference.
metadata:
  tier: open
  level: L3
  domain: real-estate
  install: optional
  keywords: [room layout, floor plan, furniture, clearance, door swing, render prompt, fixed structure]
  verified-runtimes: [claude-code]
---

# Room layout with a fixed-structure check

Layouts and renders go wrong when the walls, windows and doors quietly change. Write down what
is fixed first, then place things against it.

## Steps

1. **Fixed-structure table.** Before placing anything, list: units and scale, each wall with
   length and compass or named side, each window and door with the wall, the distance from a
   named corner, its width, and for a door the swing side and swing arc. Add a column marking
   every value `given`, `derived` or `not given`. Use one row per attribute (position, width,
   swing) so a stated attribute is never dragged to `not given` by an unstated one in the same
   row: a door that opens inward is `given` even when its hinge side is `not given`; a window
   centered on a wall is `given` even when its width is `not given`. `derived` is only for
   values you compute from given ones (the opposite wall, the swing area from the door width).
   Never write `estimated` or `assumption` in the status column, and never on anything the
   user said, including which walls are the long ones and which way an opening swings; do not
   ask the user to reconfirm stated facts. A value the user did not state is `not given`; ask
   for it or list it in a separate assumptions line, never fill it silently. If the wording of
   a stated value leaves a reading open (which edge an offset is measured to; a room "4.0 x
   3.0" read as width along the south and north walls by depth along the east and west walls),
   the value stays `given` and the reading goes in the assumptions line. The room size and the
   wall lengths are `given`, never `not given`, and you do not ask which side is longer.
2. **Photos.** A single photo without a known reference object gives no reliable dimensions;
   say so and ask for one measurement. Do not read sizes off a picture.
3. **Coordinates.** Pick an origin (a named corner) and place each piece by the distances of
   its edges from named walls, using only the sizes the user gave. Show the footprint as
   x-range and y-range. Do not invent furniture the user did not list.
4. **Clearances.** For every walkway or swing, show the arithmetic: free width = wall length
   minus the pieces along it, and the door arc as a rectangle or quarter circle in the same
   coordinates. State whether each requested clearance holds, and which piece breaks it.
5. **Compare back.** When the user supplies a generated image, render prompt output or a second
   layout, list against the table every wall, window, door or object that moved, appeared or
   vanished, and each size that no longer matches. If nothing can be checked, say what is
   missing.
6. **Fixed structure the user has not measured.** Columns, radiators, pipes, built-ins and
   beam drops that are not in the table are unknown. Name them as items to measure, not as
   absent.

## Output

The fixed-structure table, the placement table with coordinates, the clearance checks with
numbers, then moved-appeared-vanished items if a comparison was asked, and a short list of
measurements still needed. No safety, structural or code conclusions.
