import { NextResponse } from 'next/server';
import prisma from '@/service/db';

// Rows fetched per query. The whole export used to be loaded at once and then
// copied again into one string, which ran the 512 MB instance out of memory
// (and took the whole tool down) for ranges of a few months. Streaming batches
// keeps memory flat regardless of the range.
const BATCH_SIZE = 500;

const csvHeaders = [
  'Task ID',
  'Group',
  'State',
  'Batch ID',
  'Transcriber',
  'Reviewer',
  'Final Reviewer',
  'Diplomatic Context',
  'Normalised Context',
  'Corrected Context',
  'Reviewed Context',
  'Final Reviewed Context',
  'Created At',
  'Submitted At',
  'Reviewed At',
  'Final Reviewed At',
  'Duration',
  'Reviewer Rejected Count',
  'Final Reviewer Rejected Count'
];

const quote = (value) => `"${(value || '').replace(/"/g, '""')}"`;

const toCsvRow = (task) => [
  task.id,
  quote(task.group?.name),
  task.state,
  quote(task.batch_id),
  quote(task.transcriber?.name),
  quote(task.reviewer?.name),
  quote(task.final_reviewer?.name),
  quote(task.diplomatic_context),
  quote(task.normalised_context),
  quote(task.corrected_context),
  quote(task.reviewed_context),
  quote(task.final_reviewed_context),
  task.created_at?.toISOString() || '',
  task.submitted_at?.toISOString() || '',
  task.reviewed_at?.toISOString() || '',
  task.final_reviewed_at?.toISOString() || '',
  quote(task.duration),
  task.reviewer_rejected_count || 0,
  task.final_reviewer_rejected_count || 0
].join(',');

export async function POST(request) {
  try {
    const { groupId, fromDate, toDate, userEmail } = await request.json();

    if (!groupId || !fromDate || !toDate || !userEmail) {
      return NextResponse.json(
        { error: 'Group ID, from date, to date, and user email are required' },
        { status: 400 }
      );
    }

    // Verify user exists and has the correct role for download access
    const user = await prisma.user.findUnique({
      where: { email: userEmail },
      select: { role: true, id: true },
    });

    if (!user) {
      return NextResponse.json(
        { error: 'User not found' },
        { status: 401 }
      );
    }

    if (user.role !== 'REVIEWER' && user.role !== 'FINAL_REVIEWER') {
      return NextResponse.json(
        { error: 'Access denied. Only reviewers and final reviewers can download data.' },
        { status: 403 }
      );
    }

    const where = {
      group_id: parseInt(groupId),
      OR: [
        { submitted_at: { gte: new Date(fromDate), lte: new Date(toDate) } },
        { reviewed_at: { gte: new Date(fromDate), lte: new Date(toDate) } },
        { final_reviewed_at: { gte: new Date(fromDate), lte: new Date(toDate) } },
      ],
    };

    const encoder = new TextEncoder();
    let cursor = null;
    let headerSent = false;

    // Pull-based, so the next batch is only queried once the client has
    // consumed the previous one.
    const stream = new ReadableStream({
      async pull(controller) {
        try {
          if (!headerSent) {
            headerSent = true;
            controller.enqueue(encoder.encode(csvHeaders.join(',')));
          }

          const tasks = await prisma.task.findMany({
            where,
            include: {
              transcriber: { select: { name: true } },
              reviewer: { select: { name: true } },
              final_reviewer: { select: { name: true } },
              group: { select: { name: true } },
            },
            // id breaks ties so the cursor never skips or repeats a row
            orderBy: [{ created_at: 'desc' }, { id: 'desc' }],
            take: BATCH_SIZE,
            ...(cursor && { cursor: { id: cursor }, skip: 1 }),
          });

          if (tasks.length > 0) {
            controller.enqueue(encoder.encode('\n' + tasks.map(toCsvRow).join('\n')));
            cursor = tasks[tasks.length - 1].id;
          }
          if (tasks.length < BATCH_SIZE) controller.close();
        } catch (error) {
          console.error('Error streaming CSV:', error);
          controller.error(error);
        }
      },
    });

    return new Response(stream, {
      status: 200,
      headers: {
        'Content-Type': 'text/csv',
        'Content-Disposition': `attachment; filename="tasks-${groupId}-${fromDate}-${toDate}.csv"`,
      },
    });

  } catch (error) {
    console.error('Error generating CSV:', error);
    return NextResponse.json(
      { error: 'Failed to generate CSV' },
      { status: 500 }
    );
  }
}
