/**
 * OS Assignment #1
 **/

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <ctype.h>
#include <errno.h>
#include <sys/wait.h>


#define MSG(x...) fprintf (stderr, x)
#define STRERROR  strerror (errno)

#define ID_MIN 2
#define ID_MAX 8
#define COMMAND_LEN 256

typedef enum
{
  ACTION_ONCE,                        // one time
  ACTION_RESPAWN,                     // repeat

} Action;

typedef struct _Task Task;
struct _Task
{
  Task          *next;                 // next task

  volatile pid_t pid;
  int            piped;
  int            pipe_a[2];
  int            pipe_b[2];

  char           id[ID_MAX + 1];        // id
  char           pipe_id[ID_MAX + 1];   // pipe id
  int            order;                 // order
  Action         action;                // action
  char           command[COMMAND_LEN];  // command
};

static Task *tasks; // tasks
static volatile int running; 
static volatile int pid_number;

static char *strstrip (char *str) // delete space
{
  char  *start;
  size_t len;

  len = strlen (str);
  while (len--)
    {
      if (!isspace (str[len]))
        break;
      str[len] = '\0';
    }

  for (start = str; *start && isspace (*start); start++)
    ;
  memmove (str, start, strlen (start) + 1);

  return str;
}

static int check_valid_id (const char *str) // id check
{
  size_t len;
  int    i;

  len = strlen (str);
  if (len < ID_MIN || ID_MAX < len)
    return -1;

  for (i = 0; i < len; i++)
    if (!(islower (str[i]) || isdigit (str[i])))
      return -1;

  return 0;
}

static Task * lookup_task (const char *id) // find task by id
{
  Task *task;

  for (task = tasks; task != NULL; task = task->next){
    if (!strcmp (task->id, id))
      return task;
  }

  return NULL;
}

static Task * lookup_task_by_pid (pid_t pid) // find task by pid
{
  Task *task;

  for (task = tasks; task != NULL; task = task->next)
    if (task->pid == pid)
      return task;

  return NULL;
}

static void print_tasks() // print tasks for check order
{
  MSG("[%s]\n",__func__ );

  Task * tp = tasks;
  // print tasks after tp
  while(1){

    if(tp->next == NULL){
      MSG("END");
      break;
    }
    else{
      MSG("(id: %-10s, action: %d, pipe-id: %-10s, order= %-3d)\n", tp->id, tp->action, tp->pipe_id, tp->order);
      tp = tp->next; 
    }
  }

  MSG("\n\n");
}

static void append_task (Task *task) // append task by order
{
  Task * new_task;
  
  new_task = malloc (sizeof (Task)); // task 저장할 메모리 할당
  if (!new_task)
    {
      MSG ("failed to allocate a task: %s\n", STRERROR);
      return;
    }

  *new_task = *task; // new task에 task 내용을 저장
  new_task->next = NULL; // next는 우선 없도록 저장 

  if (tasks == NULL) // task가 하나도 없는 경우
    tasks = new_task;
  else
    {
      Task *tp;
      Task * prev_tp;
      tp = tasks;
      
      while(tp!= NULL){
        prev_tp = tp;
        if(tp->order <= new_task->order){
          tp = tp->next;        
        }else{
          break;
        }
      }

       // insert task by non descent order 
      new_task->next = prev_tp->next;
      prev_tp ->next = new_task;      
    }

  print_tasks();
}

static int check_valid_order(const char* str) // order check
{

  if(isdigit(str))
    return 1;
  else
    return 0;
}

static void print_sp(char *ele, char* s, char* p){
  // MSG("***%s in line[%d]\n s:[%s] p:[%s]\n\n",ele, __LINE__, s, p);
}

static int read_config (const char *filename)
{                   
  FILE *fp;
  char  line[COMMAND_LEN * 2];
  int   line_nr;

  fp = fopen (filename, "r");
  if (!fp)
    return -1;

  tasks = NULL;

  line_nr = 0;
  while (fgets (line, sizeof (line), fp))
    {
      Task   task;
      char  *p;
      char  *s;
      size_t len;

      line_nr++;
      memset (&task, 0x00, sizeof (task)); // task data init all 0

      len = strlen (line);
      if (line[len - 1] == '\n')
        line[len - 1] = '\0';

      if (0)
        MSG ("config[%3d] %s\n", line_nr, line);

      strstrip (line); 

      /* comment or empty line */
      if (line[0] == '#' || line[0] == '\0')
        continue;

      /* id */
      // -----parsing pattern start -----------
      s = line;
      p = strchr (s, ':');
      print_sp("id",s,p);

      if (!p)
        goto invalid_line;
      *p = '\0';              // ':' -> '\n' results "id\0action..." 
      strstrip (s);
      print_sp("id",s,p);
      // -----parsing pattern end ---------

      if (check_valid_id (s))
        {
          MSG ("invalid id '%s' in line %d, ignored\n", s, line_nr);
          continue;
        }
      if (lookup_task (s))
        {
          MSG ("duplicate id '%s' in line %d, ignored\n", s, line_nr);
          continue;
        }
      strcpy (task.id, s);

      /* action */

      // parsing pattern by ':' 
      s = p + 1;
      p = strchr (s, ':');
      print_sp("action",s,p);
      if (!p)
        goto invalid_line;
      *p = '\0';
      strstrip (s);
      print_sp("action",s,p);
      //-----------------------

      if (!strcasecmp (s, "once"))
        task.action = ACTION_ONCE;
      else if (!strcasecmp (s, "respawn"))
        task.action = ACTION_RESPAWN;
      else
        {
          MSG ("invalid action '%s' in line %d, ignored\n", s, line_nr);
          continue;
        }

      /* order */
      s = p + 1;
      p = strchr(s, ':');
      print_sp("order",s,p);
      if(!p)
        goto invalid_line;
      *p = '\0';
      strstrip(s);
      print_sp("order",s,p);
      
      if(s[0]=='\0')
        MSG("s is NULL");

      if(s[0] == NULL){
        task.order = 9999;
      }else if(atoi(s)){
        task.order = atoi(s);
      }else{
        MSG ("invalid order '%s' in line %d, ignored\n", s, line_nr);
        continue;
      }
      
      

      /* pipe-id */
      s = p + 1;
      p = strchr (s, ':');
      if (!p)
        goto invalid_line;
      *p = '\0';
      strstrip (s);
      if (s[0] != '\0')
        {
          Task *t;

          if (check_valid_id (s))
            {
              MSG ("invalid pipe-id '%s' in line %d, ignored\n", s, line_nr);
              continue;
            }

          t = lookup_task (s);
          if (!t)
            {
              MSG ("unknown pipe-id '%s' in line %d, ignored\n", s, line_nr);
              continue;
            }
          if (task.action == ACTION_RESPAWN || t->action == ACTION_RESPAWN)
            {
              MSG ("pipe not allowed for 'respawn' tasks in line %d, ignored\n", line_nr);
              continue;
            }
          if (t->piped)
            {
              MSG ("pipe not allowed for already piped tasks in line %d, ignored\n", line_nr);
              continue;
            }

          strcpy (task.pipe_id, s);
          task.piped = 1;
          t->piped = 1;
        }

      /* command */
      s = p + 1;
      strstrip (s);
      if (s[0] == '\0')
        {
          MSG ("empty command in line %d, ignored\n", line_nr);
          continue;
        }
      strncpy (task.command, s, sizeof (task.command) - 1);
      task.command[sizeof (task.command) - 1] = '\0';

      if (0)
        MSG ("id:%s pipe-id:%s action:%d command:%s\n",
             task.id, task.pipe_id, task.action, task.command);

      append_task (&task);
      continue;

    invalid_line:
      MSG ("invalid format in line %d, ignored\n", line_nr);
    }
  
  fclose (fp);

  return 0;
}

static char **
make_command_argv (const char *str)
{
  char      **argv;
  const char *p;
  int         n;

  for (n = 0, p = str; p != NULL; n++)
    {
      char *s;

      s = strchr (p, ' ');
      if (!s)
        break;
      p = s + 1;
    }
  n++;

  argv = calloc (sizeof (char *), n + 1);
  if (!argv)
    {
      MSG ("failed to allocate a command vector: %s\n", STRERROR);
      return NULL;
    }

  for (n = 0, p = str; p != NULL; n++)
    {
      char *s;

      s = strchr (p, ' ');
      if (!s)
        break;
      argv[n] = strndup (p, s - p);
      p = s + 1;
    }
  argv[n] = strdup (p);

  if (0)
    {

      MSG ("command:%s\n", str);
      for (n = 0; argv[n] != NULL; n++)
        MSG ("  argv[%d]:%s\n", n, argv[n]);
    }

  return argv;
}

static void
spawn_task (Task *task)
{
  if (0) MSG ("spawn program '%s'...\n", task->id);

  if (task->piped && task->pipe_id[0] == '\0')
    {
      if (pipe (task->pipe_a))
        {
          task->piped = 0;
          MSG ("failed to pipe() for prgoram '%s': %s\n", task->id, STRERROR);
        }
      if (pipe (task->pipe_b))
        {
          task->piped = 0;
          MSG ("failed to pipe() for prgoram '%s': %s\n", task->id, STRERROR);
        }
    }
  
  task->pid = fork ();
  if (task->pid < 0)
    {
      MSG ("failed to fork() for program '%s': %s\n", task->id, STRERROR);
      return;
    }

  /* child process */
  if (task->pid == 0)
    {
      char **argv;

      argv = make_command_argv (task->command);
      if (!argv || !argv[0])
        {
          MSG ("failed to parse command '%s'\n", task->command);
          exit (-1);
        }

      if (task->piped)
        {
          if (task->pipe_id[0] == '\0')
            {
              dup2 (task->pipe_a[1], 1);
              dup2 (task->pipe_b[0], 0);
              close (task->pipe_a[0]);
              close (task->pipe_a[1]);
              close (task->pipe_b[0]);
              close (task->pipe_b[1]);
            }
          else
            {
              Task *sibling;

              sibling = lookup_task (task->pipe_id);
              if (sibling && sibling->piped)
                {
                  dup2 (sibling->pipe_a[0], 0);
                  dup2 (sibling->pipe_b[1], 1);
                  close (sibling->pipe_a[0]);
                  close (sibling->pipe_a[1]);
                  close (sibling->pipe_b[0]);
                  close (sibling->pipe_b[1]);
                }
            }
        }

      execvp (argv[0], argv);
      MSG ("failed to execute command '%s': %s\n", task->command, STRERROR);
      exit (-1);
    }
}

static void
spawn_tasks (void)
{
  Task *task;

  for (task = tasks; task != NULL && running; task = task->next)
    spawn_task (task);
}

static void
wait_for_children (int signo)
{
  Task *task;
  pid_t pid;

 rewait:
  pid = waitpid (-1, NULL, WNOHANG);
  if (pid <= 0)
    return;

  task = lookup_task_by_pid (pid);
  if (!task)
    {
      MSG ("unknown pid %d", pid);
      return;
    }

  if (0) MSG ("program[%s] terminated\n", task->id);

  if (running && task->action == ACTION_RESPAWN)
    spawn_task (task);
  else
    task->pid = 0;

  /* some SIGCHLD signals is lost... */
  goto rewait;
}

static void
terminate_children (int signo)
{
  Task *task;

  if (1) MSG ("terminated by SIGNAL(%d)\n", signo);

  running = 0;

  for (task = tasks; task != NULL; task = task->next)
    if (task->pid > 0)
      {
        if (0) MSG ("kill program[%s] by SIGNAL(%d)\n", task->id, signo);
        kill (task->pid, signo);
      }

  exit (1);
}

int
main (int argc, char **argv)
{
  struct sigaction sa;
  int terminated;

  if (argc <= 1)
    {
      MSG ("usage: %s config-file\n", argv[0]);
      return -1;
    }

  if (read_config (argv[1]))
    {
      MSG ("failed to load config file '%s': %s\n", argv[1], STRERROR);
      return -1;
    }

  running = 1;

  /* SIGCHLD */
  sigemptyset (&sa.sa_mask);
  sa.sa_flags = 0;
  sa.sa_handler = wait_for_children;
  if (sigaction (SIGCHLD, &sa, NULL))
    MSG ("failed to register signal handler for SIGINT\n");

  /* SIGINT */
  sigemptyset (&sa.sa_mask);
  sa.sa_flags = 0;
  sa.sa_handler = terminate_children;
  if (sigaction (SIGINT, &sa, NULL))
    MSG ("failed to register signal handler for SIGINT\n");

  /* SIGTERM */
  sigemptyset (&sa.sa_mask);
  sa.sa_flags = 0;
  sa.sa_handler = terminate_children;
  if (sigaction (SIGTERM, &sa, NULL))
    MSG ("failed to register signal handler for SIGINT\n");

  spawn_tasks ();

  terminated = 0;
  while (!terminated)
    {
      Task *task;

      terminated = 1;
      for (task = tasks; task != NULL; task = task->next)
        if (task->pid > 0)
          {
            terminated = 0;
            break;
          }

      usleep (100000);
    }
  
  return 0;
}
